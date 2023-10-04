/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectBuilder;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.DelayedProcessor;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.AttributeId;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.DynamicAttributeUpdate;
import io.pravega.segmentstore.contracts.DynamicAttributeValue;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.IteratorState;
import io.pravega.segmentstore.contracts.tables.KeyNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
import io.pravega.segmentstore.contracts.tables.TableSegmentInfo;
import io.pravega.segmentstore.server.AttributeIterator;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Fixed-Key-Length Table Segment layout.
 *
 * This Layout supports only fixed-length keys (declared at segment creation), but is significantly less complex and
 * resource intensive than {@link HashTableSegmentLayout} and provides order (sorting) between Keys.
 */
@Beta
@Slf4j
class FixedKeyLengthTableSegmentLayout extends TableSegmentLayout {
    private final DelayedProcessor<CompactionCandidate> compactionService;
    private final TableCompactor.Config tableCompactorConfig;

    //region Constructor

    FixedKeyLengthTableSegmentLayout(Connector connector, TableExtensionConfig config, ScheduledExecutorService executor) {
        super(connector, config, executor);
        this.compactionService = new DelayedProcessor<>(this::compactIfNeeded, config.getCompactionFrequency(), executor,
                String.format("TableCompactor[%s]", connector.getContainerId()));
        this.tableCompactorConfig = new TableCompactor.Config(config.getMaxCompactionSize());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.compactionService.shutdown();
        log.info("{}: Closed.", this.traceObjectId);
    }

    //endregion

    //region TableSegmentLayout Implementation

    @Override
    Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata) {
        // We don't need any Writer Processors.
        return Collections.emptyList();
    }

    @Override
    Map<AttributeId, Long> getNewSegmentAttributes(@NonNull TableSegmentConfig config) {
        ensureValidKeyLength("", config.getKeyLength());
        val result = new HashMap<AttributeId, Long>();
        result.put(Attributes.ATTRIBUTE_ID_LENGTH, (long) config.getKeyLength());
        result.putAll(this.config.getDefaultCompactionAttributes());
        if (config.getRolloverSizeBytes() > 0) {
            result.put(Attributes.ROLLOVER_SIZE, config.getRolloverSizeBytes());
        }
        return result;
    }

    @Override
    CompletableFuture<Void> deleteSegment(@NonNull String segmentName, boolean mustBeEmpty, Duration timeout) {
        if (mustBeEmpty) {
            throw new UnsupportedOperationException("mustBeEmpty not supported on Fixed-Key-Length Table Segments.");
        }

        return this.connector.deleteSegment(segmentName, timeout)
                .thenRun(() -> this.compactionService.cancel(segmentName));
    }

    @Override
    CompletableFuture<List<Long>> put(@NonNull DirectSegmentAccess segment, @NonNull List<TableEntry> entries, long tableSegmentOffset, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        val segmentKeyLength = getSegmentKeyLength(segmentInfo);
        ensureValidKeyLength(segmentInfo.getName(), segmentKeyLength);

        val attributeUpdates = new AttributeUpdateCollection();
        int batchOffset = 0;
        val batchOffsets = new ArrayList<Integer>();
        boolean isConditional = false;
        for (val e : entries) {
            val key = e.getKey();
            Preconditions.checkArgument(key.getKey().getLength() == segmentKeyLength,
                    "Entry Key Length for key `%s` incompatible with segment '%s' which requires key lengths of %s.",
                    key, segmentInfo.getName(), segmentKeyLength);

            attributeUpdates.add(createIndexUpdate(key, batchOffset));
            isConditional |= key.hasVersion();
            batchOffsets.add(batchOffset);
            batchOffset += this.serializer.getUpdateLength(e);
        }

        logRequest("put", segmentInfo.getName(), isConditional, tableSegmentOffset, entries.size(), batchOffset);
        if (batchOffset > this.config.getMaxBatchSize()) {
            throw new UpdateBatchTooLargeException(batchOffset, this.config.getMaxBatchSize());
        }

        // Update total number of entries in Table (this includes updates to the same key).
        attributeUpdates.add(new AttributeUpdate(TableAttributes.TOTAL_ENTRY_COUNT, AttributeUpdateType.Accumulate, entries.size()));

        val serializedEntries = this.serializer.serializeUpdate(entries);
        val append = tableSegmentOffset == TableSegmentLayout.NO_OFFSET
                ? segment.append(serializedEntries, attributeUpdates, timer.getRemaining())
                : segment.append(serializedEntries, attributeUpdates, tableSegmentOffset, timer.getRemaining());
        return handleConditionalUpdateException(append, segmentInfo)
                .thenApply(segmentOffset -> {
                    this.compactionService.process(new CompactionCandidate(segment));
                    return batchOffsets.stream().map(offset -> offset + segmentOffset).collect(Collectors.toList());
                });
    }

    @Override
    CompletableFuture<Void> remove(@NonNull DirectSegmentAccess segment, @NonNull Collection<TableKey> keys, long tableSegmentOffset, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        val segmentKeyLength = getSegmentKeyLength(segmentInfo);
        ensureValidKeyLength(segmentInfo.getName(), segmentKeyLength);

        val attributeUpdates = new AttributeUpdateCollection();
        boolean isConditional = false;
        for (val key : keys) {
            Preconditions.checkArgument(key.getKey().getLength() == segmentKeyLength,
                    "Key Length for key `%s` incompatible with segment '%s' which requires key lengths of %s.",
                    key, segmentInfo.getName(), segmentKeyLength);
            attributeUpdates.add(createIndexRemoval(key));
            isConditional |= key.hasVersion();
        }

        logRequest("remove", segmentInfo.getName(), isConditional, tableSegmentOffset, keys.size());
        val result = tableSegmentOffset == NO_OFFSET
                ? segment.updateAttributes(attributeUpdates, timer.getRemaining())
                : segment.append(BufferView.empty(), attributeUpdates, tableSegmentOffset, timer.getRemaining());
        return handleConditionalUpdateException(result, segmentInfo)
                .thenRun(() -> this.compactionService.process(new CompactionCandidate(segment)));
    }

    @Override
    CompletableFuture<List<TableEntry>> get(@NonNull DirectSegmentAccess segment, @NonNull List<BufferView> keys, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        val attributes = keys.stream().map(key -> {
            ensureValidKeyLength(segmentInfo.getName(), key.getLength());
            return AttributeId.from(key.getCopy());
        }).collect(Collectors.toList());

        logRequest("get", segmentInfo.getName(), keys.size());
        return getByAttributes(segment, attributes, timer);
    }

    private CompletableFuture<List<TableEntry>> getByAttributes(DirectSegmentAccess segment, List<AttributeId> attributes, TimeoutTimer timer) {
        return segment.getAttributes(attributes, false, timer.getRemaining())
                .thenComposeAsync(attributeValues -> {
                    val result = new ArrayList<CompletableFuture<TableEntry>>(attributes.size());
                    for (val attributeId : attributes) {
                        val segmentOffset = attributeValues.getOrDefault(attributeId, NO_OFFSET);
                        result.add(getEntryWithSingleRetry(segment, attributeId, segmentOffset, timer));
                    }
                    return Futures.allOfWithResults(result);
                }, this.executor);
    }

    private CompletableFuture<TableEntry> getEntryWithSingleRetry(DirectSegmentAccess segment, AttributeId attributeId,
                                                                  long segmentOffset, TimeoutTimer timer) {
        return Futures.exceptionallyComposeExpecting(
                getEntry(segment, attributeId, segmentOffset, timer),
                ex -> ex instanceof StreamSegmentTruncatedException,
                () -> {
                    log.debug("{}: Offset {} truncated while reading key '{}'. Retrying once.", this.traceObjectId, segmentOffset, attributeId);
                    return segment.getAttributes(Collections.singletonList(attributeId), false, timer.getRemaining())
                            .thenComposeAsync(attributeValues ->
                                    getEntry(segment, attributeId, attributeValues.getOrDefault(attributeId, NO_OFFSET), timer));
                });
    }

    private CompletableFuture<TableEntry> getEntry(@NonNull DirectSegmentAccess segment, AttributeId attributeId,
                                                   long segmentOffset, TimeoutTimer timer) {
        if (segmentOffset < 0) {
            return CompletableFuture.completedFuture(null);
        }
        val entryReader = AsyncTableEntryReader.readEntry(attributeId.toBuffer(), segmentOffset, this.serializer, timer);
        val readResult = segment.read(segmentOffset, EntrySerializer.MAX_SERIALIZATION_LENGTH, timer.getRemaining());
        AsyncReadResultProcessor.process(readResult, entryReader, this.executor);
        return entryReader.getResult();
    }

    @Override
    CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args) {
        logRequest("keyIterator", segment.getInfo().getName(), args);
        return newIterator(segment, this::getIteratorKeys, args);
    }

    @Override
    CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args) {
        logRequest("entryIterator", segment.getInfo().getName(), args);
        return newIterator(segment, this::getIteratorEntries, args);
    }

    @Override
    AsyncIterator<IteratorItem<TableEntry>> entryDeltaIterator(@NonNull DirectSegmentAccess segment, long fromPosition, Duration fetchTimeout) {
        // We do not support delta iterators for this layout. If needed, we can always implement it.
        throw new UnsupportedOperationException("entryDeltaIterator");
    }

    @Override
    CompletableFuture<TableSegmentInfo> getInfo(@NonNull DirectSegmentAccess segment, Duration timeout) {
        val m = segment.getInfo();
        return segment.getExtendedAttributeCount(timeout)
                .thenApply(entryCount -> TableSegmentInfo.builder()
                        .name(m.getName())
                        .length(m.getLength())
                        .startOffset(m.getStartOffset())
                        .type(m.getType())
                        .entryCount(entryCount)
                        .keyLength(getSegmentKeyLength(m))
                        .build());
    }

    //endregion

    //region Helpers

    private AttributeUpdate createIndexUpdate(TableKey key, int batchOffset) {
        val attributeId = AttributeId.from(key.getKey().getCopy());
        val ref = DynamicAttributeValue.segmentLength(batchOffset);
        return key.hasVersion()
                ? new DynamicAttributeUpdate(attributeId, AttributeUpdateType.ReplaceIfEquals, ref, getCompareVersion(key))
                : new DynamicAttributeUpdate(attributeId, AttributeUpdateType.Replace, ref);
    }

    private AttributeUpdate createIndexRemoval(TableKey key) {
        val attributeId = AttributeId.from(key.getKey().getCopy());
        return key.hasVersion()
                ? new AttributeUpdate(attributeId, AttributeUpdateType.ReplaceIfEquals, Attributes.NULL_ATTRIBUTE_VALUE, getCompareVersion(key))
                : new AttributeUpdate(attributeId, AttributeUpdateType.Replace, Attributes.NULL_ATTRIBUTE_VALUE);
    }

    private <T> CompletableFuture<T> handleConditionalUpdateException(CompletableFuture<T> update, SegmentProperties segmentInfo) {
        return update.exceptionally(ex -> {
            ex = Exceptions.unwrap(ex);
            if (ex instanceof BadAttributeUpdateException) {
                val bau = (BadAttributeUpdateException) ex;
                ex = bau.isPreviousValueMissing()
                        ? new KeyNotExistsException(segmentInfo.getName(), bau.getAttributeId().toBuffer())
                        : new BadKeyVersionException(segmentInfo.getName(), Collections.emptyMap());
            }

            throw new CompletionException(ex);
        });
    }

    private CompletableFuture<Void> compactIfNeeded(CompactionCandidate candidate) {
        val compactor = new FixedKeyLengthTableCompactor(candidate.getSegment(), this.tableCompactorConfig, this.executor);
        val timer = new TimeoutTimer(this.config.getRecoveryTimeout());
        return compactor.isCompactionRequired()
                .thenComposeAsync(isRequired -> {
                    if (isRequired) {
                        return compact(candidate.getSegment(), compactor, timer);
                    } else {
                        log.debug("{}: No compaction required at this time.", this.traceObjectId);
                        return CompletableFuture.completedFuture(null);
                    }
                }, this.executor);
    }

    private CompletableFuture<Void> compact(DirectSegmentAccess segment, FixedKeyLengthTableCompactor compactor, TimeoutTimer timer) {
        return compactor.compact(timer)
                .thenComposeAsync(v -> {
                    val metadata = segment.getInfo();
                    val truncateOffset = compactor.calculateTruncationOffset(-1L);
                    if (truncateOffset > metadata.getStartOffset()) {
                        log.debug("{}: Truncating segment at offset {}.", this.traceObjectId, truncateOffset);
                        return segment.truncate(truncateOffset, timer.getRemaining());
                    } else {
                        log.debug("{}: No segment truncation possible now.", this.traceObjectId);
                        return CompletableFuture.completedFuture(null);
                    }
                }, this.executor);
    }

    private <T> CompletableFuture<AsyncIterator<IteratorItem<T>>> newIterator(@NonNull DirectSegmentAccess segment,
                                                                              @NonNull GetIteratorItem<T> getItems,
                                                                              @NonNull IteratorArgs args) {
        Preconditions.checkArgument(args.getContinuationToken() == null, "ContinuationToken not supported for FixedKeyLengthTableSegments.");
        val segmentKeyLength = getSegmentKeyLength(segment.getInfo());
        val fromId = args.getFrom() == null
                ? AttributeId.Variable.minValue(segmentKeyLength)
                : AttributeId.from(args.getFrom().getCopy());

        val toId = args.getTo() == null
                ? AttributeId.Variable.maxValue(segmentKeyLength)
                : AttributeId.from(args.getTo().getCopy());
        val timer = new TimeoutTimer(args.getFetchTimeout());
        return segment.attributeIterator(fromId, toId, timer.getRemaining())
                .thenApply(ai -> new TableIterator<>(ai, segment, getItems, timer));
    }

    private void ensureSegmentType(String segmentName, SegmentType segmentType) {
        Preconditions.checkArgument(segmentType.isFixedKeyLengthTableSegment(),
                "FixedKeyTableSegmentLayout can only be used for Fixed-Key Table Segments; Segment '%s' is '%s;.", segmentName, segmentType);
    }

    private void ensureValidKeyLength(String segmentName, int keyLength) {
        Preconditions.checkArgument(keyLength > 0 && keyLength <= AttributeId.MAX_LENGTH,
                "Segment KeyLength for segment `%s' must be a positive integer smaller than or equal to %s; actual %s.",
                segmentName, AttributeId.MAX_LENGTH, keyLength);
    }

    private int getSegmentKeyLength(SegmentProperties segmentInfo) {
        return (int) (long) segmentInfo.getAttributes().getOrDefault(Attributes.ATTRIBUTE_ID_LENGTH, -1L);
    }

    private CompletableFuture<List<TableKey>> getIteratorKeys(DirectSegmentAccess segment, List<Map.Entry<AttributeId, Long>> keys, TimeoutTimer timer) {
        return CompletableFuture.completedFuture(keys.stream().map(e -> TableKey.versioned(e.getKey().toBuffer(), e.getValue())).collect(Collectors.toList()));
    }

    private CompletableFuture<List<TableEntry>> getIteratorEntries(DirectSegmentAccess segment, List<Map.Entry<AttributeId, Long>> keys, TimeoutTimer timer) {
        return get(segment, keys.stream().map(e -> e.getKey().toBuffer()).collect(Collectors.toList()), timer);
    }

    private long getCompareVersion(TableKey key) {
        return key.getVersion() == TableKey.NOT_EXISTS ? Attributes.NULL_ATTRIBUTE_VALUE : key.getVersion();
    }

    //endregion

    //region Helper Classes

    @Data
    private static class CompactionCandidate implements DelayedProcessor.Item {
        private final DirectSegmentAccess segment;

        @Override
        public String key() {
            return this.segment.getInfo().getName();
        }
    }

    @RequiredArgsConstructor
    private class TableIterator<T> implements AsyncIterator<IteratorItem<T>> {
        private final AttributeIterator attributeIterator;
        private final DirectSegmentAccess segment;
        private final GetIteratorItem<T> getItems;
        private final TimeoutTimer timer;

        @Override
        public CompletableFuture<IteratorItem<T>> getNext() {
            return getNextAttributes()
                    .thenCompose(attributePairs -> {
                        // Create a new state.
                        val stateBuilder = IteratorStateImpl.builder();
                        CompletableFuture<List<T>> result;
                        if (attributePairs == null) {
                            // We are done.
                            return CompletableFuture.completedFuture(null);
                        } else {
                            stateBuilder.lastKey(attributePairs.get(attributePairs.size() - 1).getKey().toBuffer());
                            result = this.getItems.apply(this.segment, attributePairs, this.timer);
                        }

                        return result.thenApply(items -> new IteratorItemImpl<>(stateBuilder.build().serialize(), items));
                    });
        }

        private CompletableFuture<List<Map.Entry<AttributeId, Long>>> getNextAttributes() {
            val result = new AtomicReference<List<Map.Entry<AttributeId, Long>>>();
            val retry = new AtomicBoolean(true);
            return Futures.loop(
                    retry::get,
                    this.attributeIterator::getNext,
                    p -> {
                        result.set(p);
                        if (p == null) {
                            retry.set(false);
                        } else {
                            p.removeIf(e -> e.getValue() == Attributes.NULL_ATTRIBUTE_VALUE);
                            retry.set(p.isEmpty());
                        }
                    },
                    executor)
                    .thenApply(v -> result.get());
        }
    }

    @FunctionalInterface
    private interface GetIteratorItem<T> {
        CompletableFuture<List<T>> apply(DirectSegmentAccess segment, List<Map.Entry<AttributeId, Long>> keys, TimeoutTimer timer);
    }

    /**
     * Represents the state of a resumable iterator.
     */
    @AllArgsConstructor
    @Builder
    @Getter
    public static class IteratorStateImpl implements IteratorState {
        private static final Serializer SERIALIZER = new Serializer();
        private final BufferView lastKey;

        @Override
        public String toString() {
            return String.format("LastKey = %s", this.lastKey);
        }

        //region Serialization

        /**
         * Creates a new instance of the IteratorState class from the given array.
         *
         * @param data The serialization of an IteratorState. This must have been generated using {@link #serialize()}.
         * @return As new instance of the IteratorState class.
         * @throws IOException If unable to deserialize.
         */
        static IteratorStateImpl deserialize(BufferView data) throws IOException {
            return SERIALIZER.deserialize(data);
        }

        /**
         * Serializes this IteratorState instance into an {@link ArrayView}.
         *
         * @return The {@link ArrayView} that was used for serialization.
         */
        @Override
        @SneakyThrows(IOException.class)
        public ArrayView serialize() {
            return SERIALIZER.serialize(this);
        }

        private static class IteratorStateImplBuilder implements ObjectBuilder<IteratorStateImpl> {

        }

        private static class Serializer extends VersionedSerializer.WithBuilder<IteratorStateImpl, IteratorStateImpl.IteratorStateImplBuilder> {
            @Override
            protected IteratorStateImplBuilder newBuilder() {
                return new IteratorStateImplBuilder();
            }

            @Override
            protected byte getWriteVersion() {
                return 0;
            }

            @Override
            protected void declareVersions() {
                version(0).revision(0, this::write00, this::read00);
            }

            private void read00(RevisionDataInput revisionDataInput, IteratorStateImplBuilder builder) throws IOException {
                builder.lastKey = new ByteArraySegment(revisionDataInput.readArray());
                if (builder.lastKey.getLength() == 0) {
                    builder.lastKey = null;
                }
            }

            private void write00(IteratorStateImpl state, RevisionDataOutput revisionDataOutput) throws IOException {
                revisionDataOutput.writeBuffer(state.lastKey);
            }
        }

        //endregion
    }

    //endregion
}
