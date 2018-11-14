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
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.server.AttributeIterator;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Iterates through {@link TableBucket}s in a Segment.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
class TableBucketIterator implements AsyncIterator<List<TableBucket>> {
    private final AttributeIterator indexIterator;
    private final ArrayDeque<Map.Entry<UUID, Long>> cacheHashes;
    private final ScheduledExecutorService executor;

    static AsyncBuilder builder() {
        return new AsyncBuilder();
    }

    //region AsyncIterator Implementation

    @Override
    public CompletableFuture<List<TableBucket>> getNext() {
        return this.indexIterator.getNext()
                                 .thenApplyAsync(this::process, this.executor);
    }

    private List<TableBucket> process(List<Map.Entry<UUID, Long>> indexHashes) {
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

    static class AsyncBuilder {
        private DirectSegmentAccess segment;
        private Map<UUID, Long> cacheHashes;
        private UUID firstHash;
        private ScheduledExecutorService executor;
        private Duration fetchTimeout;

        AsyncBuilder segment(@NonNull DirectSegmentAccess segment) {
            this.segment = segment;
            return this;
        }

        AsyncBuilder cacheHashes(@NonNull Map<UUID, Long> cacheHashes) {
            this.cacheHashes = cacheHashes;
            return this;
        }

        AsyncBuilder firstHash(UUID firstHash) {
            Preconditions.checkArgument(firstHash == null || KeyHasher.isValid(firstHash), "Invalid firstHash.");
            this.firstHash = firstHash;
            return this;
        }

        AsyncBuilder executor(@NonNull ScheduledExecutorService executor) {
            this.executor = executor;
            return this;
        }

        AsyncBuilder fetchTimeout(@NonNull Duration fetchTimeout) {
            this.fetchTimeout = fetchTimeout;
            return this;
        }

        CompletableFuture<TableBucketIterator> build() {
            ArrayDeque<Map.Entry<UUID, Long>> cacheHashes;
            CompletableFuture<AttributeIterator> aiFuture;
            if (this.firstHash == null) {
                // Nothing to iterate on.
                aiFuture = CompletableFuture.completedFuture(() -> null);
                cacheHashes = new ArrayDeque<>();
            } else {
                // Sort the Cache Hashes and get the Attribute Iterator.
                cacheHashes = getCacheHashes(this.cacheHashes, this.firstHash);
                aiFuture = this.segment.attributeIterator(this.firstHash, KeyHasher.MAX_HASH, this.fetchTimeout);
            }
            return aiFuture.thenApply(attributeIterator -> new TableBucketIterator(attributeIterator, cacheHashes, this.executor));
        }

        private ArrayDeque<Map.Entry<UUID, Long>> getCacheHashes(Map<UUID, Long> unindexedKeyHashes, UUID firstHash) {
            // Filter out the Hashes which are below our first hash, then sort them.
            return unindexedKeyHashes.entrySet().stream()
                                     .filter(e -> e.getKey().compareTo(firstHash) >= 0)
                                     .sorted(Comparator.comparing(Map.Entry::getKey))
                                     .collect(Collectors.toCollection(ArrayDeque::new));
        }
    }
}
