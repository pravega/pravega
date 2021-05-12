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
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.serialization.RevisionDataInput;
import io.pravega.common.io.serialization.RevisionDataOutput;
import io.pravega.common.io.serialization.VersionedSerializer;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
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
import io.pravega.segmentstore.contracts.tables.BadKeyVersionException;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.IteratorState;
import io.pravega.segmentstore.contracts.tables.KeyNotExistsException;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableSegmentConfig;
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
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

/**
 * Fixed-Key-Length Table Segment layout.
 *
 * This Layout supports only fixed-length keys (declared at segment creation), but is significantly less complex and
 * resource intensive than {@link HashTableSegmentLayout} and provides order (sorting) between Keys.
 */
@Beta
class FixedKeyLengthTableSegmentLayout extends TableSegmentLayout {
    //region Constructor

    FixedKeyLengthTableSegmentLayout(Connector connector, TableExtensionConfig config, ScheduledExecutorService executor) {
        super(connector, config, executor);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        // Nothing to do.
    }

    //endregion

    //region TableSegmentLayout Implementation

    @Override
    Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata) {
        // We don't need any Writer Processors. TODO: how about compaction?
        return Collections.emptyList();
    }

    @Override
    Map<AttributeId, Long> getNewSegmentAttributes(@NonNull TableSegmentConfig config) {
        ensureValidKeyLength("", config.getKeyLength());
        Preconditions.checkArgument(config.getKeyLength() % Long.BYTES == 0, "KeyLength must be a multiple of %s; given %s.", Long.BYTES, config.getKeyLength());
        val result = new HashMap<AttributeId, Long>();
        result.put(Attributes.ATTRIBUTE_ID_LENGTH, (long) config.getKeyLength());
        result.putAll(this.config.getDefaultCompactionAttributes());
        return result;
    }

    @Override
    CompletableFuture<Void> deleteSegment(@NonNull String segmentName, boolean mustBeEmpty, Duration timeout) {
        Preconditions.checkArgument(!mustBeEmpty, "mustBeEmpty not supported on Fixed-Key Table Segments.");
        return this.connector.deleteSegment(segmentName, timeout);
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
        for (val e : entries) {
            val key = e.getKey();
            Preconditions.checkArgument(key.getKey().getLength() == segmentKeyLength,
                    "Entry Key Length for key `%s` incompatible with segment '%s' which requires key lengths of %s.",
                    key, segmentInfo.getName(), segmentKeyLength);

            val attributeId = AttributeId.from(key.getKey().getCopy());
            val ref = DynamicAttributeValue.segmentLength(batchOffset);
            val au = key.hasVersion()
                    ? new DynamicAttributeUpdate(attributeId, AttributeUpdateType.ReplaceIfEquals, ref, getCompareVersion(key))
                    : new DynamicAttributeUpdate(attributeId, AttributeUpdateType.Replace, ref);
            attributeUpdates.add(au);
            batchOffsets.add(batchOffset);
            batchOffset += this.serializer.getUpdateLength(e);
        }

        if (batchOffset > this.config.getMaxBatchSize()) {
            throw new UpdateBatchTooLargeException(batchOffset, this.config.getMaxBatchSize());
        }

        val serializedEntries = this.serializer.serializeUpdate(entries);
        val append = tableSegmentOffset == TableSegmentLayout.NO_OFFSET
                ? segment.append(serializedEntries, attributeUpdates, timer.getRemaining())
                : segment.append(serializedEntries, attributeUpdates, tableSegmentOffset, timer.getRemaining());
        return handleConditionalUpdateException(append, segmentInfo)
                .thenApply(segmentOffset -> batchOffsets.stream().map(offset -> offset + segmentOffset).collect(Collectors.toList()));
    }

    @Override
    CompletableFuture<Void> remove(@NonNull DirectSegmentAccess segment, @NonNull Collection<TableKey> keys, long tableSegmentOffset, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        val segmentKeyLength = getSegmentKeyLength(segmentInfo);
        ensureValidKeyLength(segmentInfo.getName(), segmentKeyLength);

        val attributeUpdates = new AttributeUpdateCollection();
        for (val key : keys) {
            Preconditions.checkArgument(key.getKey().getLength() == segmentKeyLength,
                    "Key Length for key `%s` incompatible with segment '%s' which requires key lengths of %s.",
                    key, segmentInfo.getName(), segmentKeyLength);

            val attributeId = AttributeId.from(key.getKey().getCopy());
            val au = key.hasVersion()
                    ? new AttributeUpdate(attributeId, AttributeUpdateType.ReplaceIfEquals, Attributes.NULL_ATTRIBUTE_VALUE, getCompareVersion(key))
                    : new AttributeUpdate(attributeId, AttributeUpdateType.Replace, Attributes.NULL_ATTRIBUTE_VALUE);
            attributeUpdates.add(au);
        }
        val result = tableSegmentOffset == NO_OFFSET
                ? segment.updateAttributes(attributeUpdates, timer.getRemaining())
                : segment.append(BufferView.empty(), attributeUpdates, tableSegmentOffset, timer.getRemaining());
        return Futures.toVoid(handleConditionalUpdateException(result, segmentInfo));
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

    @Override
    CompletableFuture<List<TableEntry>> get(@NonNull DirectSegmentAccess segment, @NonNull List<BufferView> keys, TimeoutTimer timer) {
        val segmentInfo = segment.getInfo();
        ensureSegmentType(segmentInfo.getName(), segmentInfo.getType());
        val attributes = keys.stream().map(key -> {
            ensureValidKeyLength(segmentInfo.getName(), key.getLength());
            return AttributeId.from(key.getCopy());
        }).collect(Collectors.toList());

        return segment.getAttributes(attributes, false, timer.getRemaining())
                .thenComposeAsync(attributeValues -> {
                    val result = new ArrayList<CompletableFuture<TableEntry>>(attributes.size());
                    for (val attributeId : attributes) {
                        val segmentOffset = attributeValues.getOrDefault(attributeId, NO_OFFSET);
                        if (segmentOffset < 0) {
                            result.add(CompletableFuture.completedFuture(null));
                        } else {
                            // TODO: whenever we do compaction, account for StreamSegmentTruncatedException.
                            val entryReader = AsyncTableEntryReader.readEntry(attributeId.toBuffer(), segmentOffset, this.serializer, timer);
                            val readResult = segment.read(segmentOffset, EntrySerializer.MAX_SERIALIZATION_LENGTH, timer.getRemaining());
                            AsyncReadResultProcessor.process(readResult, entryReader, this.executor);
                            result.add(entryReader.getResult());
                        }
                    }
                    return Futures.allOfWithResults(result);
                }, this.executor);
    }

    @Override
    CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args) {
        return newIterator(segment, this::getIteratorKeys, args);
    }

    @Override
    CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(@NonNull DirectSegmentAccess segment, IteratorArgs args) {
        return newIterator(segment, this::getIteratorEntries, args);
    }

    @Override
    AsyncIterator<IteratorItem<TableEntry>> entryDeltaIterator(@NonNull DirectSegmentAccess segment, long fromPosition, Duration fetchTimeout) {
        // We do not support delta iterators for this layout. If needed, we can always implement it.
        throw new UnsupportedOperationException("entryDeltaIterator");
    }

    //endregion

    //region Helpers

    private <T> CompletableFuture<AsyncIterator<IteratorItem<T>>> newIterator(@NonNull DirectSegmentAccess segment,
                                                                              @NonNull GetIteratorItem<T> getItems,
                                                                              @NonNull IteratorArgs args) {
        Preconditions.checkArgument(args.getPrefixFilter() == null, "PrefixFilter not supported.");
        IteratorStateImpl state = null;
        if (args.getSerializedState() != null) {
            try {
                state = IteratorStateImpl.deserialize(args.getSerializedState());
            } catch (IOException ex) {
                // Bad IteratorState serialization.
                throw new IllegalDataFormatException("Unable to deserialize `serializedState`.", ex);
            }
        }

        val segmentKeyLength = getSegmentKeyLength(segment.getInfo());
        val fromId = state == null
                ? AttributeId.Variable.minValue(segmentKeyLength)
                : AttributeId.from(state.getLastKey().getCopy()).nextValue();
        if (fromId == null) {
            // We've reached the end.
            return CompletableFuture.completedFuture(TableIterator.empty());
        }

        val toId = AttributeId.Variable.maxValue(segmentKeyLength);
        val timer = new TimeoutTimer(args.getFetchTimeout());
        return segment.attributeIterator(fromId, toId, timer.getRemaining())
                .thenApply(ai -> ai.thenCompose(attributePairs -> {
                    // Create a new state.
                    val stateBuilder = IteratorStateImpl.builder();
                    CompletableFuture<List<T>> result;
                    if (attributePairs.isEmpty()) {
                        stateBuilder.lastKey(AttributeId.Variable.maxValue(segmentKeyLength).toBuffer());
                        result = CompletableFuture.completedFuture(Collections.emptyList());
                    } else {
                        stateBuilder.lastKey(attributePairs.get(attributePairs.size() - 1).getKey().toBuffer());
                        result = getItems.apply(segment, attributePairs, timer);
                    }

                    // TODO: truncated segment (if compaction)
                    return result.thenApply(items -> new IteratorItemImpl<>(stateBuilder.build().serialize(), items));
                }));
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

    @FunctionalInterface
    private interface GetIteratorItem<T> {
        CompletableFuture<List<T>> apply(DirectSegmentAccess segment, List<Map.Entry<AttributeId, Long>> keys, TimeoutTimer timer);
    }

    /**
     * Represents the state of a resumable iterator.
     */
    @RequiredArgsConstructor
    @Builder
    @Getter
    private static class IteratorStateImpl implements IteratorState {
        private static final Serializer SERIALIZER = new Serializer();

        @NonNull
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
            }

            private void write00(IteratorStateImpl state, RevisionDataOutput revisionDataOutput) throws IOException {
                revisionDataOutput.writeBuffer(state.lastKey);
            }
        }

        //endregion
    }

    //endregion

}
