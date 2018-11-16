/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.base.Preconditions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.server.AttributeIterator;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Iterates through {@link TableBucket}s in a Segment.
 * @param <T> Type of the final, converted result.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class TableIterator<T> implements AsyncIterator<T> {
    //region Members

    private final AttributeIterator indexHashIterator;
    private final ConvertResult<T> resultConverter;
    private final ArrayDeque<Map.Entry<UUID, Long>> cacheHashes;
    private final Executor executor;
    private final AtomicReference<Iterator<TableBucket>> currentBatch = new AtomicReference<>();

    //endregion

    //region AsyncIterator Implementation

    @Override
    public CompletableFuture<T> getNext() {
        return getNextBucket()
                .thenCompose(bucket -> {
                    if (bucket == null) {
                        // We are done.
                        return CompletableFuture.completedFuture(null);
                    } else {
                        // Convert the TableBucket into the desired result.
                        return this.resultConverter.apply(bucket);
                    }
                });
    }

    /**
     * Gets the next {@link TableBucket} in the iteration. This will either be served directly from the cached batch
     * of {@link TableBucket}s or a new invocation to the underlying indexHashIterator will be performed to fetch the next
     * {@link TableBucket}.
     */
    private CompletableFuture<TableBucket> getNextBucket() {
        val fromBatch = getNextBucketFromExistingBatch();
        if (fromBatch != null) {
            return CompletableFuture.completedFuture(fromBatch);
        }

        val canContinue = new AtomicBoolean(true);
        return Futures
                .loop(
                        canContinue::get,
                        this::fetchNextTableBuckets,
                        canContinue::set,
                        this.executor)
                .thenApply(v -> getNextBucketFromExistingBatch());
    }

    /**
     * Gets the next {@link TableBucket} in the iteration from the cached batch. If the batch is exhausted, returns null.
     */
    private TableBucket getNextBucketFromExistingBatch() {
        val batch = this.currentBatch.get();
        if (batch != null) {
            TableBucket next = batch.next();
            if (!batch.hasNext()) {
                this.currentBatch.set(null);
            }

            return next;
        }

        return null;
    }

    /**
     * Fetches the next set of {@link TableBucket}s from the indexHashIterator.
     */
    private CompletableFuture<Boolean> fetchNextTableBuckets() {
        return this.indexHashIterator
                .getNext()
                .thenApplyAsync(indexHashes -> {
                    List<TableBucket> buckets = toBuckets(indexHashes);
                    if (buckets == null) {
                        // End of iteration.
                        return false;
                    } else if (!buckets.isEmpty()) {
                        // Got something. Stop here for now.
                        this.currentBatch.set(buckets.iterator());
                        return false;
                    }
                    return true;
                }, this.executor);
    }

    /**
     * Merges the given list of Index Hashes with the available Cache Hashes and generates the associated {@link TableBucket}s.
     */
    private List<TableBucket> toBuckets(List<Map.Entry<UUID, Long>> indexHashes) {
        val buckets = new ArrayList<TableBucket>();
        if (indexHashes == null) {
            // Nothing more in the index. Add whatever is in the cache attributes.
            while (!this.cacheHashes.isEmpty()) {
                add(this.cacheHashes.removeFirst(), buckets);
            }

            return buckets.isEmpty() ? null : buckets;
        } else {
            // Transform every eligible Attribute into a TableBucket and add it to the result.
            for (val indexHash : indexHashes) {
                if (KeyHasher.isValid(indexHash.getKey()) && indexHash.getValue() != Attributes.NULL_ATTRIBUTE_VALUE) {
                    // For each bucket returned above, include all Buckets/hashes from the ContainerKeyIndex which are equal to or
                    // below it. (this is very similar to the AttributeMixer - maybe reuse that methodology).
                    boolean overridden = false;
                    while (!this.cacheHashes.isEmpty()) {
                        val cacheHash = this.cacheHashes.peekFirst();
                        int cmp = indexHash.getKey().compareTo(this.cacheHashes.peekFirst().getKey());
                        if (cmp < 0) {
                            // Cache Hash is after Index Hash. We are done here.
                            break;
                        }

                        // The value we got from the Index was overridden by the one in the cache.
                        overridden = overridden || cmp == 0;
                        add(cacheHash, buckets);
                        this.cacheHashes.removeFirst();
                    }

                    if (!overridden) {
                        add(indexHash, buckets);
                    }
                }
            }
        }

        return buckets;
    }

    private void add(Map.Entry<UUID, Long> bucketInfo, List<TableBucket> buckets) {
        buckets.add(new TableBucket(bucketInfo.getKey(), bucketInfo.getValue()));
    }

    //endregion

    //region Builder

    /**
     * Creates a new {@link TableIterator.Builder} that can be used to construct {@link TableIterator} instances.
     *
     * @param <T> Type of the elements returned at each iteration.
     * @return A new instance of the {@link TableIterator.Builder} class.
     */
    static <T> Builder<T> builder() {
        return new Builder<>();
    }

    /**
     * Creates a new {@link TableIterator} that contains no elements.
     *
     * @param <T> Type of elements returned at each iteration.
     * @return A new instance of the {@link TableIterator.Builder} class.
     */
    static <T> TableIterator<T> empty() {
        return new TableIterator<>(
                () -> CompletableFuture.completedFuture(null),
                ignored -> CompletableFuture.completedFuture(null),
                new ArrayDeque<>(),
                ForkJoinPool.commonPool());
    }

    /**
     * Builder for the {@link TableIterator} class.
     */
    static class Builder<T> {
        private DirectSegmentAccess segment;
        private Map<UUID, Long> cacheHashes;
        private UUID firstHash;
        private ConvertResult<T> resultConverter;
        private ScheduledExecutorService executor;
        private Duration fetchTimeout;

        /**
         * Sets a {@link DirectSegmentAccess} representing a Table Segment that the iterator will iterate over.
         *
         * @param segment The {@link DirectSegmentAccess} to associate.
         * @return This object.
         */
        Builder<T> segment(@NonNull DirectSegmentAccess segment) {
            this.segment = segment;
            return this;
        }

        /**
         * Sets the Key Hashes that are currently cached. These values will augment and/or supersede the values loaded
         * from the Table Segment's Index.
         *
         * @param cacheHashes A Map of Key Hashes to Segment Offsets.
         * @return This object.
         */
        Builder<T> cacheHashes(@NonNull Map<UUID, Long> cacheHashes) {
            this.cacheHashes = cacheHashes;
            return this;
        }

        /**
         * Sets the first Key Hash to begin iteration at. The first element returned by this iterator will have a Key Hash
         * equal to or greater than this one.
         *
         * @param firstHash The Key Hash.
         * @return This object.
         */
        Builder<T> firstHash(@NonNull UUID firstHash) {
            Preconditions.checkArgument(KeyHasher.isValid(firstHash), "Invalid firstHash.");
            this.firstHash = firstHash;
            return this;
        }

        /**
         * Sets the Executor to use for async operations.
         *
         * @param executor The Executor to set.
         * @return This object.
         */
        Builder<T> executor(@NonNull ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }

        /**
         * Sets a Duration representing the Timeout for each invocation to {@link TableIterator#getNext()}.
         *
         * @param fetchTimeout Timeout to set.
         * @return This object.
         */
        Builder<T> fetchTimeout(@NonNull Duration fetchTimeout) {
            this.fetchTimeout = fetchTimeout;
            return this;
        }

        /**
         * Sets a {@link ConvertResult} function that will translate each {@link TableBucket} instance into the desired
         * final result.
         *
         * @param resultConverter A Function that will translate each {@link TableBucket} instance into the desired
         *                        final result.
         * @return This object.
         */
        Builder<T> resultConverter(@NonNull ConvertResult<T> resultConverter) {
            this.resultConverter = resultConverter;
            return this;
        }

        /**
         * Creates a new instance of the {@link TableIterator} class using the information collected in this Builder.
         *
         * @return A CompletableFuture that, when completed, will contain the desired {@link TableIterator} instance.
         */
        CompletableFuture<AsyncIterator<T>> build() {
            // Sort the Cache Hashes and get the Attribute Iterator.
            val cacheHashes = getCacheHashes(this.cacheHashes, this.firstHash);
            val aiFuture = this.segment.attributeIterator(this.firstHash, KeyHasher.MAX_HASH, this.fetchTimeout);
            return aiFuture.thenApply(attributeIterator ->
                    new TableIterator<>(attributeIterator, this.resultConverter, cacheHashes, this.executor));
        }

        private ArrayDeque<Map.Entry<UUID, Long>> getCacheHashes(Map<UUID, Long> unindexedKeyHashes, UUID firstHash) {
            // Filter out the Hashes which are below our first hash, then sort them.
            return unindexedKeyHashes.entrySet().stream()
                                     .filter(e -> e.getKey().compareTo(firstHash) >= 0)
                                     .sorted(Comparator.comparing(Map.Entry::getKey))
                                     .collect(Collectors.toCollection(ArrayDeque::new));
        }
    }

    @FunctionalInterface
    interface ConvertResult<T> {
        CompletableFuture<T> apply(TableBucket bucket);
    }

    //endregion
}
