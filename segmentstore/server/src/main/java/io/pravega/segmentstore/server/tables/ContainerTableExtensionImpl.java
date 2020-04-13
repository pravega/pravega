/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Runnables;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.IllegalDataFormatException;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentTruncatedException;
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
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
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

    private static final int MAX_BATCH_SIZE = 32 * EntrySerializer.MAX_SERIALIZATION_LENGTH;
    /**
     * The default value to supply to a {@link WriterTableProcessor} to indicate how big compactions need to be.
     * We need to return a value that is large enough to encompass the largest possible Table Entry (otherwise
     * compaction will stall), but not too big, as that will introduce larger indexing pauses when compaction is running.
     */
    private static final int DEFAULT_MAX_COMPACTION_SIZE = 4 * EntrySerializer.MAX_SERIALIZATION_LENGTH;
    private final SegmentContainer segmentContainer;
    private final ScheduledExecutorService executor;
    private final KeyHasher hasher;
    private final ContainerSortedKeyIndex sortedKeyIndex;
    private final ContainerKeyIndex keyIndex;
    private final EntrySerializer serializer;
    private final AtomicBoolean closed;
    private final String traceObjectId;

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
        this(segmentContainer, cacheManager, KeyHasher.sha256(), executor);
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
    ContainerTableExtensionImpl(@NonNull SegmentContainer segmentContainer, @NonNull CacheManager cacheManager,
                                @NonNull KeyHasher hasher, @NonNull ScheduledExecutorService executor) {
        this.segmentContainer = segmentContainer;
        this.executor = executor;
        this.hasher = hasher;
        this.sortedKeyIndex = createSortedIndex();
        this.keyIndex = new ContainerKeyIndex(segmentContainer.getId(), cacheManager, this.sortedKeyIndex, this.hasher, this.executor);
        this.serializer = new EntrySerializer();
        this.closed = new AtomicBoolean();
        this.traceObjectId = String.format("TableExtension[%d]", this.segmentContainer.getId());
    }

    private ContainerSortedKeyIndex createSortedIndex() {
        val ds = new SortedKeyIndexDataSource(
                (s, entries, timeout) -> put(s, entries, false, timeout),
                (s, keys, timeout) -> remove(s, keys, false, timeout),
                (s, keys, timeout) -> get(s, keys, false, timeout));
        return new ContainerSortedKeyIndex(ds, this.executor);
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
    public CompletableFuture<Void> createSegment(@NonNull String segmentName, boolean sorted, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        val attributes = new HashMap<>(TableAttributes.DEFAULT_VALUES);
        if (sorted) {
            attributes.put(TableAttributes.SORTED, Attributes.BOOLEAN_TRUE);
        }

        val attributeUpdates = attributes
                .entrySet().stream()
                .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.None, e.getValue()))
                .collect(Collectors.toList());
        logRequest("createSegment", segmentName);
        return this.segmentContainer.createStreamSegment(segmentName, attributeUpdates, timeout);
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
        return put(segmentName, entries, true, timeout);
    }

    private CompletableFuture<List<Long>> put(@NonNull String segmentName, @NonNull List<TableEntry> entries, boolean external, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> {
                    val segmentInfo = segment.getInfo();
                    val toUpdate = translateItems(entries, segmentInfo, external, KeyTranslator::inbound);

                    // Generate an Update Batch for all the entries (since we need to know their Key Hashes and relative
                    // offsets in the batch itself).
                    val updateBatch = batch(toUpdate, TableEntry::getKey, this.serializer::getUpdateLength, TableKeyBatch.update());
                    logRequest("put", segmentInfo.getName(), updateBatch.isConditional(), updateBatch.isRemoval(),
                            toUpdate.size(), updateBatch.getLength());
                    return this.keyIndex.update(segment, updateBatch,
                            () -> commit(toUpdate, updateBatch.getLength(), this.serializer::serializeUpdate, segment, timer.getRemaining()), timer);
                }, this.executor);
    }

    @Override
    public CompletableFuture<Void> remove(@NonNull String segmentName, @NonNull Collection<TableKey> keys, Duration timeout) {
        return remove(segmentName, keys, true, timeout);
    }

    private CompletableFuture<Void> remove(@NonNull String segmentName, @NonNull Collection<TableKey> keys, boolean external, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentContainer
                .forSegment(segmentName, timer.getRemaining())
                .thenComposeAsync(segment -> {
                    val segmentInfo = segment.getInfo();
                    val toRemove = translateItems(keys, segmentInfo, external, KeyTranslator::inbound);
                    val removeBatch = batch(toRemove, key -> key, this.serializer::getRemovalLength, TableKeyBatch.removal());
                    logRequest("remove", segmentInfo.getName(), removeBatch.isConditional(), removeBatch.isRemoval(),
                            toRemove.size(), removeBatch.getLength());
                    return this.keyIndex.update(segment, removeBatch,
                            () -> commit(toRemove, removeBatch.getLength(), this.serializer::serializeRemoval, segment, timer.getRemaining()), timer);
                }, this.executor)
                .thenRun(Runnables.doNothing());
    }

    @Override
    public CompletableFuture<List<TableEntry>> get(@NonNull String segmentName, @NonNull List<ArrayView> keys, Duration timeout) {
        return get(segmentName, keys, true, timeout);
    }

    private CompletableFuture<List<TableEntry>> get(@NonNull String segmentName, @NonNull List<ArrayView> keys, boolean external, Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        logRequest("get", segmentName, keys.size());
        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        } else {
            TimeoutTimer timer = new TimeoutTimer(timeout);
            return this.segmentContainer
                    .forSegment(segmentName, timer.getRemaining())
                    .thenComposeAsync(segment -> {
                        val segmentInfo = segment.getInfo();
                        val toGet = translateItems(keys, segmentInfo, external, KeyTranslator::inbound);
                        val resultBuilder = new GetResultBuilder(toGet, this.hasher);
                        return this.keyIndex.getBucketOffsets(segment, resultBuilder.getHashes(), timer)
                                .thenComposeAsync(offsets -> get(segment, resultBuilder, offsets, timer), this.executor)
                                .thenApply(results -> translateItems(results, segmentInfo, external, KeyTranslator::outbound));
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
                // We first attempt an optimistic read, which involves fewer steps, and only invalidate the cache and read
                // directly from the index if unable to find anything and there is a chance the sought key actually exists.
                // Encountering a truncated Segment offset indicates that the Segment may have recently been compacted and
                // we are using a stale cache value.
                ArrayView key = builder.getKeys().get(i);
                builder.includeResult(Futures
                        .exceptionallyExpecting(bucketReader.find(key, offset, timer), ex -> ex instanceof StreamSegmentTruncatedException, null)
                        .thenComposeAsync(entry -> {
                            if (entry != null) {
                                // We found an entry; need to figure out if it was a deletion or not.
                                return CompletableFuture.completedFuture(maybeDeleted(entry));
                            } else {
                                // We have a valid TableBucket but were unable to locate the key using the cache, either
                                // because the cache points to a truncated offset or because we are unable to determine
                                // if the TableBucket has been rearranged due to a compaction. The rearrangement is a rare
                                // occurrence and can only happen if more than one Key is mapped to a bucket (collision).
                                return this.keyIndex.getBucketOffsetDirect(segment, keyHash, timer)
                                        .thenComposeAsync(newOffset -> bucketReader.find(key, newOffset, timer), this.executor)
                                        .thenApply(this::maybeDeleted);
                            }
                        }, this.executor));
            }
        }

        return builder.getResultFutures();
    }

    @SuppressWarnings("unchecked")
    private <T, V extends Collection<T>> V translateItems(V items, SegmentProperties segmentInfo, boolean isExternal,
                                                          BiFunction<KeyTranslator, T, T> translateItem) {
        if (!ContainerSortedKeyIndex.isSortedTableSegment(segmentInfo)) {
            // Nothing to translate for non-sorted segments.
            return items;
        }

        val t = isExternal ? SortedKeyIndexDataSource.EXTERNAL_TRANSLATOR : SortedKeyIndexDataSource.INTERNAL_TRANSLATOR;
        return (V) items.stream().map(i -> translateItem.apply(t, i)).collect(Collectors.toList());
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(String segmentName, IteratorArgs args) {
        return this.segmentContainer.forSegment(segmentName, args.getFetchTimeout())
                .thenComposeAsync(segment -> {
                    if (ContainerSortedKeyIndex.isSortedTableSegment(segment.getInfo())) {
                        logRequest("keyIterator", segmentName, "sorted");
                        return newSortedIterator(segment, args,
                                keys -> CompletableFuture.completedFuture(keys.stream().map(TableKey::unversioned).collect(Collectors.toList())));
                    } else {
                        logRequest("keyIterator", segmentName, "hash");
                        return newHashIterator(segment, args, TableBucketReader::key, KeyTranslator::outbound);
                    }
                }, this.executor);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(String segmentName, IteratorArgs args) {
        return this.segmentContainer.forSegment(segmentName, args.getFetchTimeout())
                .thenComposeAsync(segment -> {
                    if (ContainerSortedKeyIndex.isSortedTableSegment(segment.getInfo())) {
                        logRequest("entryIterator", segmentName, "sorted");
                        return newSortedIterator(segment, args, keys -> get(segmentName, keys, args.getFetchTimeout()));
                    } else {
                        logRequest("entryIterator", segmentName, "hash");
                        return newHashIterator(segment, args, TableBucketReader::entry, KeyTranslator::outbound);
                    }
                }, this.executor);
    }

    //endregion

    //region Helpers

    /**
     * When overridden in a derived class, this will indicate how much to compact at each step. By default this returns
     * {@link #DEFAULT_MAX_COMPACTION_SIZE}.
     *
     * @return The maximum length to compact at each step.
     */
    @VisibleForTesting
    protected int getMaxCompactionSize() {
        return DEFAULT_MAX_COMPACTION_SIZE;
    }

    private <T> TableKeyBatch batch(Collection<T> toBatch, Function<T, TableKey> getKey, Function<T, Integer> getLength, TableKeyBatch batch) {
        for (T item : toBatch) {
            val length = getLength.apply(item);
            val key = getKey.apply(item);
            batch.add(key, this.hasher.hash(key.getKey()), length);
        }

        Preconditions.checkArgument(batch.getLength() <= MAX_BATCH_SIZE,
                "Update Batch length (%s) exceeds the maximum limit.", MAX_BATCH_SIZE);
        return batch;
    }

    private <T> CompletableFuture<Long> commit(Collection<T> toCommit, int serializationLength, BiConsumer<Collection<T>, byte[]> serializer,
                                               DirectSegmentAccess segment, Duration timeout) {
        assert serializationLength <= MAX_BATCH_SIZE;
        byte[] s = new byte[serializationLength];
        serializer.accept(toCommit, s);
        return segment.append(new ByteArraySegment(s), null, timeout);
    }

    private <T> CompletableFuture<AsyncIterator<IteratorItem<T>>> newSortedIterator(@NonNull DirectSegmentAccess segment, @NonNull IteratorArgs args,
                                                                                    @NonNull Function<List<ArrayView>, CompletableFuture<List<T>>> toResult) {
        return this.keyIndex.getSortedKeyIndex(segment)
                .thenApply(index -> {
                    val range = index.getIteratorRange(args.getSerializedState(), args.getPrefixFilter());
                    return index.iterator(range, args.getFetchTimeout())
                            .thenCompose(keys -> toSortedIteratorItem(keys, toResult, segment.getInfo()));
                });
    }

    private <T> CompletableFuture<IteratorItem<T>> toSortedIteratorItem(List<ArrayView> keys, Function<List<ArrayView>,
            CompletableFuture<List<T>>> toResult, SegmentProperties segmentInfo) {
        if (keys == null || keys.isEmpty()) {
            // End of iteration.
            return CompletableFuture.completedFuture(null);
        }

        // Remember the last key before the translation. We'll send this with the response so we know where to resume next.
        val lastKey = keys.get(keys.size() - 1);

        // Convert the Keys to their external form.
        keys = translateItems(keys, segmentInfo, true, KeyTranslator::outbound);

        // Get the result and include it in the response.
        return toResult.apply(keys)
                .thenApply(result -> {
                    // Some elements may have been deleted in the meantime, so exclude them.
                    result = result.stream().filter(Objects::nonNull).collect(Collectors.toList());
                    return new IteratorItemImpl<>(lastKey, result);
                });
    }

    private <T> CompletableFuture<AsyncIterator<IteratorItem<T>>> newHashIterator(@NonNull DirectSegmentAccess segment, @NonNull IteratorArgs args,
                                                                                  @NonNull GetBucketReader<T> createBucketReader,
                                                                                  @NonNull BiFunction<KeyTranslator, T, T> translateItem) {
        Preconditions.checkArgument(args.getPrefixFilter() == null, "Cannot perform a KeyHash iteration with a prefix.");
        UUID fromHash;
        try {
            fromHash = KeyHasher.getNextHash(args.getSerializedState() == null ? null : IteratorState.deserialize(args.getSerializedState()).getKeyHash());
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
                        .thenApply(result -> {
                            result = translateItems(result, segmentInfo, true, translateItem);
                            return new IteratorItemImpl<>(new IteratorState(bucket.getHash()).serialize(), result);
                        });

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
        public SegmentSortedKeyIndex getSortedKeyIndex() {
            return ContainerTableExtensionImpl.this.sortedKeyIndex.getSortedKeyIndex(this.metadata.getId(), this.metadata);
        }

        @Override
        public CompletableFuture<DirectSegmentAccess> getSegment(Duration timeout) {
            return ContainerTableExtensionImpl.this.segmentContainer.forSegment(this.metadata.getName(), timeout);
        }

        @Override
        public void notifyIndexOffsetChanged(long lastIndexedOffset) {
            ContainerTableExtensionImpl.this.keyIndex.notifyIndexOffsetChanged(this.metadata.getId(), lastIndexedOffset);
        }

        @Override
        public int getMaxCompactionSize() {
            return ContainerTableExtensionImpl.this.getMaxCompactionSize();
        }

        @Override
        public void close() {
            // Tell the KeyIndex that it's ok to clear any tail-end cache.
            ContainerTableExtensionImpl.this.keyIndex.notifyIndexOffsetChanged(this.metadata.getId(), -1L);
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
        private final List<ArrayView> keys;
        /**
         * Sought keys's hashes, in the same order as the keys.
         */
        @Getter
        private final List<UUID> hashes;

        /**
         * A list of Futures with the results for each key, in the same order as the keys.
         */
        private final List<CompletableFuture<TableEntry>> resultFutures;

        GetResultBuilder(List<ArrayView> keys, KeyHasher hasher) {
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
        private final ArrayView state;
        private final Collection<T> entries;
    }

    @FunctionalInterface
    private interface GetBucketReader<T> {
        TableBucketReader<T> apply(DirectSegmentAccess segment, TableBucketReader.GetBackpointer getBackpointer, ScheduledExecutorService executor);
    }

    //endregion
}
