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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Runnables;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.IllegalDataFormatException;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * A {@link ContainerTableExtension} that implements Table Segments on top of a {@link SegmentContainer}.
 */
@Slf4j
public class ContainerTableExtensionImpl implements ContainerTableExtension {
    //region Members

    /**
     * Default value used for when no offset is provided for a remove or put call.
     */
    private static final int NO_OFFSET = -1;

    private final SegmentContainer segmentContainer;
    private final ScheduledExecutorService executor;
    private final KeyHasher hasher;
    private final ContainerKeyIndex keyIndex;
    private final EntrySerializer serializer;
    private final AtomicBoolean closed;
    private final String traceObjectId;
    @Getter
    private final TableExtensionConfig config;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ContainerTableExtensionImpl class.
     *
     * @param segmentContainer The {@link SegmentContainer} to associate with.
     * @param cacheManager     The {@link CacheManager} to use to manage the cache.
     * @param executor         An Executor to use for async tasks.
     */
    public ContainerTableExtensionImpl(SegmentContainer segmentContainer, CacheManager cacheManager, ScheduledExecutorService executor) {
        this(TableExtensionConfig.builder().build(), segmentContainer, cacheManager, KeyHasher.sha256(), executor);
    }

    /**
     * Creates a new instance of the ContainerTableExtensionImpl class with custom {@link KeyHasher}.
     *
     * @param segmentContainer The {@link SegmentContainer} to associate with.
     * @param cacheManager     The {@link CacheManager} to use to manage the cache.
     * @param hasher           The {@link KeyHasher} to use.
     * @param executor         An Executor to use for async tasks.
     */
    @VisibleForTesting
    ContainerTableExtensionImpl(@NonNull TableExtensionConfig config, @NonNull SegmentContainer segmentContainer,
                                @NonNull CacheManager cacheManager, @NonNull KeyHasher hasher, @NonNull ScheduledExecutorService executor) {
        this.config = config;
        this.segmentContainer = segmentContainer;
        this.executor = executor;
        this.hasher = hasher;
        this.keyIndex = new ContainerKeyIndex(segmentContainer.getId(), this.config, cacheManager, this.hasher, this.executor);
        this.serializer = new EntrySerializer();
        this.closed = new AtomicBoolean();
        this.traceObjectId = String.format("TableExtension[%d]", this.segmentContainer.getId());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.getAndSet(true)) {
            this.keyIndex.close();
            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region ContainerTableExtension Implementation

    @Override
    public Collection<WriterSegmentProcessor> createWriterSegmentProcessors(UpdateableSegmentMetadata metadata) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        if (!metadata.getAttributes().containsKey(TableAttributes.INDEX_OFFSET)) {
            // Not a Table Segment; nothing to do.
            return Collections.emptyList();
        }

        return Collections.singletonList(new WriterTableProcessor(new TableWriterConnectorImpl(metadata), this.executor));
    }

    //endregion

    //region TableStore Implementation

    @Override
    public CompletableFuture<Void> createSegment(@NonNull String segmentName, SegmentType segmentType, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        val attributes = new HashMap<>(TableAttributes.DEFAULT_VALUES);
        attributes.putAll(this.config.getDefaultCompactionAttributes());

        // Fetch defaults for all attributes, but check our own DEFAULT_ATTRIBUTES for any meaningful overrides.
        // NOTE: At the moment, all TableSegments are internal to Pravega and are used for metadata storage. As such, all
        // these defaults make sense for such use cases. If TableSegments are exposed to the end-user, then this method
        // will need to accept external configuration that defines at least MIN_UTILIZATION.
        val attributeUpdates = attributes
                .entrySet().stream()
                .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.None, e.getValue()))
                .collect(Collectors.toList());
        segmentType = SegmentType.builder(segmentType).tableSegment().build(); // Ensure at least a TableSegment type.
        logRequest("createSegment", segmentName, segmentType);
        return this.segmentContainer.createStreamSegment(segmentName, segmentType, attributeUpdates, timeout);
    }

    @Override
    public CompletableFuture<Void> deleteSegment(@NonNull String segmentName, boolean mustBeEmpty, Duration timeout) {
        logRequest("deleteSegment", segmentName, mustBeEmpty);
        if (mustBeEmpty) {
            TimeoutTimer timer = new TimeoutTimer(timeout);
            return this.segmentContainer
                    .forSegment(segmentName, timer.getRemaining())
                    .thenComposeAsync(segment -> this.keyIndex.executeIfEmpty(segment,
                            () -> this.segmentContainer.deleteStreamSegment(segmentName, timer.getRemaining()), timer),
                            this.executor);
        }

        Exceptions.checkNotClosed(this.closed.get(), this);
        return this.segmentContainer.deleteStreamSegment(segmentName, timeout);
    }

    @Override
    public CompletableFuture<Void> merge(@NonNull String targetSegmentName, @NonNull String sourceSegmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        throw new UnsupportedOperationException("merge");
    }

    @Override
    public CompletableFuture<Void> seal(String segmentName, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        throw new UnsupportedOperationException("seal");
    }

    @Override
    public CompletableFuture<List<Long>> put(@NonNull String segmentName, @NonNull List<TableEntry> entries, Duration timeout) {
        return put(segmentName, entries, NO_OFFSET, timeout);
    }

    @Override
    public CompletableFuture<List<Long>> put(@NonNull String segmentName, @NonNull List<TableEntry> entries, long tableSegmentOffset, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> {
                    val segmentInfo = segment.getInfo();

                    // Generate an Update Batch for all the entries (since we need to know their Key Hashes and relative
                    // offsets in the batch itself).
                    val updateBatch = batch(entries, TableEntry::getKey, this.serializer::getUpdateLength, TableKeyBatch.update());
                    logRequest("put", segmentInfo.getName(), updateBatch.isConditional(), tableSegmentOffset, updateBatch.isRemoval(),
                            entries.size(), updateBatch.getLength());
                    return this.keyIndex.update(segment, updateBatch,
                            () -> commit(entries, this.serializer::serializeUpdate, segment, tableSegmentOffset, timer.getRemaining()), timer);
                }, this.executor);
    }

    @Override
    public CompletableFuture<Void> remove(@NonNull String segmentName, @NonNull Collection<TableKey> keys, Duration timeout) {
        return remove(segmentName, keys, NO_OFFSET, timeout);
    }

    @Override
    public CompletableFuture<Void> remove(@NonNull String segmentName, @NonNull Collection<TableKey> keys, long tableSegmentOffset, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> {
                    val segmentInfo = segment.getInfo();
                    val removeBatch = batch(keys, key -> key, this.serializer::getRemovalLength, TableKeyBatch.removal());
                    logRequest("remove", segmentInfo.getName(), removeBatch.isConditional(), removeBatch.isRemoval(),
                            keys.size(), removeBatch.getLength());
                    return this.keyIndex.update(segment, removeBatch,
                            () -> commit(keys, this.serializer::serializeRemoval, segment, tableSegmentOffset, timer.getRemaining()), timer);
                }, this.executor)
                .thenRun(Runnables.doNothing());
    }

    @Override
    public CompletableFuture<List<TableEntry>> get(@NonNull String segmentName, @NonNull List<BufferView> keys, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        logRequest("get", segmentName, keys.size());
        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        } else {
            TimeoutTimer timer = new TimeoutTimer(timeout);
            return this.segmentContainer
                    .forSegment(segmentName, timer.getRemaining())
                    .thenComposeAsync(segment -> {
                        val resultBuilder = new GetResultBuilder(keys, this.hasher);
                        return this.keyIndex.getBucketOffsets(segment, resultBuilder.getHashes(), timer)
                                .thenComposeAsync(offsets -> get(segment, resultBuilder, offsets, timer), this.executor);
                    }, this.executor);
        }
    }

    private CompletableFuture<List<TableEntry>> get(DirectSegmentAccess segment, GetResultBuilder builder,
                                                    Map<UUID, Long> bucketOffsets, TimeoutTimer timer) {
        val bucketReader = TableBucketReader.entry(segment, this.keyIndex::getBackpointerOffset, this.executor);
        int resultSize = builder.getHashes().size();
        for (int i = 0; i < resultSize; i++) {
            UUID keyHash = builder.getHashes().get(i);
            long offset = bucketOffsets.get(keyHash);
            if (offset == TableKey.NOT_EXISTS) {
                // Bucket does not exist, hence neither does the key.
                builder.includeResult(CompletableFuture.completedFuture(null));
            } else {
                // Find the sought entry in the segment, based on its key.
                BufferView key = builder.getKeys().get(i);
                builder.includeResult(this.keyIndex.findBucketEntry(segment, bucketReader, key, offset, timer).thenApply(this::maybeDeleted));
            }
        }

        return builder.getResultFutures();
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(String segmentName, IteratorArgs args) {
        logRequest("keyIterator", segmentName, "hash");
        return this.segmentContainer.forSegment(segmentName, args.getFetchTimeout())
                .thenComposeAsync(segment -> newHashIterator(segment, args, TableBucketReader::key), this.executor);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(String segmentName, IteratorArgs args) {
        logRequest("entryIterator", segmentName, "hash");
        return this.segmentContainer.forSegment(segmentName, args.getFetchTimeout())
                .thenComposeAsync(segment -> newHashIterator(segment, args, TableBucketReader::entry), this.executor);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryDeltaIterator(String segmentName, long fromPosition, Duration fetchTimeout) {
        logRequest("entryDeltaIterator", segmentName);
        return newDeltaIterator(segmentName, fromPosition, fetchTimeout);
    }

    //endregion

    //region Helpers

    private <T> TableKeyBatch batch(Collection<T> toBatch, Function<T, TableKey> getKey, Function<T, Integer> getLength, TableKeyBatch batch) {
        for (T item : toBatch) {
            val length = getLength.apply(item);
            val key = getKey.apply(item);
            batch.add(key, this.hasher.hash(key.getKey()), length);
        }

        if (batch.getLength() > this.config.getMaxBatchSize()) {
            throw new UpdateBatchTooLargeException(batch.getLength(), this.config.getMaxBatchSize());
        }
        return batch;
    }

    private <T> CompletableFuture<Long> commit(Collection<T> toCommit, Function<Collection<T>, BufferView> serializer,
                                               DirectSegmentAccess segment, long tableSegmentOffset, Duration timeout) {
        BufferView s = serializer.apply(toCommit);
        if (tableSegmentOffset == NO_OFFSET) {
            return segment.append(s, null, timeout);
        } else {
            return segment.append(s, null, tableSegmentOffset, timeout);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<AsyncIterator<IteratorItem<T>>> newDeltaIterator(@NonNull String segmentName, long fromPosition, @NonNull Duration fetchTimeout) {
        return this.segmentContainer
                .forSegment(segmentName, fetchTimeout)
                .thenComposeAsync(segment -> {
                    SegmentProperties properties = segment.getInfo();
                    if (fromPosition > properties.getLength()) {
                        throw new IllegalArgumentException("fromPosition can not exceed the length of the TableSegment.");
                    }
                    long compactionOffset = properties.getAttributes().getOrDefault(TableAttributes.COMPACTION_OFFSET, 0L);
                    // All of the most recent keys will exist beyond the compactionOffset.
                    long startOffset = Math.max(fromPosition, compactionOffset);
                    // We should clear if the starting position may have been truncated out due to compaction.
                    boolean shouldClear = fromPosition < compactionOffset;
                    // Maximum length of the TableSegment we want to read until.
                    int maxBytesToRead = (int) (properties.getLength() - startOffset);
                    TableEntryDeltaIterator.ConvertResult<IteratorItem<T>> converter = item -> {
                        return CompletableFuture.completedFuture(new IteratorItemImpl<T>(
                                item.getKey().serialize(),
                                Collections.singletonList((T) item.getValue())));
                    };
                    val iterator = TableEntryDeltaIterator.<IteratorItem<T>>builder()
                            .segment(segment)
                            .entrySerializer(serializer)
                            .executor(executor)
                            .maxBytesToRead(maxBytesToRead)
                            .startOffset(startOffset)
                            .currentBatchOffset(fromPosition)
                            .fetchTimeout(fetchTimeout)
                            .resultConverter(converter)
                            .shouldClear(shouldClear)
                            .build();

                    return CompletableFuture.completedFuture(iterator);
                }, this.executor);
    }

    private <T> CompletableFuture<AsyncIterator<IteratorItem<T>>> newHashIterator(@NonNull DirectSegmentAccess segment, @NonNull IteratorArgs args,
                                                                                  @NonNull GetBucketReader<T> createBucketReader) {
        Preconditions.checkArgument(args.getPrefixFilter() == null, "Cannot perform a KeyHash iteration with a prefix.");
        UUID fromHash;
        try {
            fromHash = KeyHasher.getNextHash(args.getSerializedState() == null ? null : IteratorStateImpl.deserialize(args.getSerializedState()).getKeyHash());
        } catch (IOException ex) {
            // Bad IteratorState serialization.
            throw new IllegalDataFormatException("Unable to deserialize `serializedState`.", ex);
        }

        if (fromHash == null) {
            // Nothing to iterate on.
            return CompletableFuture.completedFuture(TableIterator.empty());
        }

        // Create a converter that will use a TableBucketReader to fetch all requested items in the iterated Buckets.
        val bucketReader = createBucketReader.apply(segment, this.keyIndex::getBackpointerOffset, this.executor);
        val segmentInfo = segment.getInfo();
        TableIterator.ConvertResult<IteratorItem<T>> converter = bucket ->
                bucketReader.findAllExisting(bucket.getSegmentOffset(), new TimeoutTimer(args.getFetchTimeout()))
                        .thenApply(result -> new IteratorItemImpl<>(new IteratorStateImpl(bucket.getHash()).serialize(), result));

        // Fetch the Tail (Unindexed) Hashes, then create the TableIterator.
        return this.keyIndex.getUnindexedKeyHashes(segment)
                .thenComposeAsync(cacheHashes -> TableIterator.<IteratorItem<T>>builder()
                        .segment(segment)
                        .cacheHashes(cacheHashes)
                        .firstHash(fromHash)
                        .executor(executor)
                        .resultConverter(converter)
                        .fetchTimeout(args.getFetchTimeout())
                        .build(), this.executor);
    }

    private TableEntry maybeDeleted(TableEntry e) {
        return e == null || e.getValue() == null ? null : e;
    }

    private void logRequest(String requestName, Object... args) {
        log.debug("{}: {} {}", this.traceObjectId, requestName, args);
    }

    //endregion

    //region TableWriterConnector

    @RequiredArgsConstructor
    private class TableWriterConnectorImpl implements TableWriterConnector {
        @Getter
        private final SegmentMetadata metadata;

        @Override
        public EntrySerializer getSerializer() {
            return ContainerTableExtensionImpl.this.serializer;
        }

        @Override
        public KeyHasher getKeyHasher() {
            return ContainerTableExtensionImpl.this.hasher;
        }

        @Override
        public CompletableFuture<DirectSegmentAccess> getSegment(Duration timeout) {
            // IMPORTANT: we only invoke forSegment with OperationPriority.Critical here - the WriterTableProcessor is the
            // only one that requires this priority as it runs within the StorageWriter. We should not be prioritizing
            // the other operations (such as put() or remove()) as those are user-generated.
            return ContainerTableExtensionImpl.this.segmentContainer.forSegment(this.metadata.getName(), OperationPriority.Critical, timeout);
        }

        @Override
        public void notifyIndexOffsetChanged(long lastIndexedOffset, int processedSizeBytes) {
            ContainerTableExtensionImpl.this.keyIndex.notifyIndexOffsetChanged(this.metadata.getId(), lastIndexedOffset, processedSizeBytes);
        }

        @Override
        public int getMaxCompactionSize() {
            return ContainerTableExtensionImpl.this.config.getMaxCompactionSize();
        }

        @Override
        public void close() {
            // Tell the KeyIndex that it's ok to clear any tail-end cache.
            ContainerTableExtensionImpl.this.keyIndex.notifyIndexOffsetChanged(this.metadata.getId(), -1L, 0);
        }
    }

    //endregion

    //region GetResultBuilder

    /**
     * Helps build Result for the {@link #get} method.
     */
    private static class GetResultBuilder {
        /**
         * Sought keys.
         */
        @Getter
        private final List<BufferView> keys;
        /**
         * Sought keys's hashes, in the same order as the keys.
         */
        @Getter
        private final List<UUID> hashes;

        /**
         * A list of Futures with the results for each key, in the same order as the keys.
         */
        private final List<CompletableFuture<TableEntry>> resultFutures;

        GetResultBuilder(List<BufferView> keys, KeyHasher hasher) {
            this.keys = keys;
            this.hashes = keys.stream().map(hasher::hash).collect(Collectors.toList());
            this.resultFutures = new ArrayList<>();
        }

        void includeResult(CompletableFuture<TableEntry> entryFuture) {
            this.resultFutures.add(entryFuture);
        }

        CompletableFuture<List<TableEntry>> getResultFutures() {
            return Futures.allOfWithResults(this.resultFutures);
        }
    }

    //endregion

    //region IteratorItemImpl

    @Data
    private static class IteratorItemImpl<T> implements IteratorItem<T> {
        private final BufferView state;
        private final Collection<T> entries;
    }

    @FunctionalInterface
    private interface GetBucketReader<T> {
        TableBucketReader<T> apply(DirectSegmentAccess segment, TableBucketReader.GetBackpointer getBackpointer, ScheduledExecutorService executor);
    }

    //endregion

    //region UpdateBatchTooLargeException

    @VisibleForTesting
    static class UpdateBatchTooLargeException extends IllegalArgumentException {
        UpdateBatchTooLargeException(int length, int maxLength) {
            super(String.format("Update Batch length %s exceeds the maximum limit %s.", length, maxLength));
        }
    }

    //endregion
}
