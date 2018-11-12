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

import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Iterates through {@link TableBucket}s in a Segment.
 */
class TableBucketIterator implements AsyncIterator<IteratorItem<TableBucket>> {
    private static final int MIN_ITERATION_BATCH = 10; // Should we accept this via the input?
    private final DirectSegmentAccess segment;
    private final AtomicReference<IteratorState> state;
    private final ScheduledExecutorService executor;
    private final Duration fetchTimeout;
    private final ArrayDeque<UnindexedKey> unindexedKeys;

    TableBucketIterator(@NonNull DirectSegmentAccess segment, @NonNull List<HashedArray> unindexedKeyHashes, @NonNull KeyHasher hasher,
                        IteratorState initialState, @NonNull ScheduledExecutorService executor, @NonNull Duration fetchTimeout) {
        this.segment = segment;
        this.state = new AtomicReference<>(initialState);
        this.executor = executor;
        this.fetchTimeout = fetchTimeout;
        this.unindexedKeys = getUnindexedKeys(unindexedKeyHashes, hasher);
    }

    //region AsyncIterator Implementation

    @Override
    public CompletableFuture<IteratorItem<TableBucket>> getNext() {
        IteratorState state = this.state.get();
        if(state != null && state.isEnd()){
            // We are done.
            return null;
        }

        // We will not split the Primary Hash across multiple iterations; that is, if we include at least one Table Bucket
        // with a particular Primary Hash, then we must include all Table Buckets with that Primary Hash. It is unlikely
        // that keys will collide on the Primary Hash so, in general, there shouldn't be more than one level anyway.

        // Determine the next Primary Hash to query.
        UUID nextPrimaryHashKey = getNextPrimaryHash();
        ArrayView nextPrimaryHash = this.attributeCalculator.getPrimaryHash(nextPrimaryHashKey);

        // Filter out all those Unindexed Keys which have a PH Key smaller than nextPrimaryHashKey.
        while (!this.unindexedKeys.isEmpty() && nextPrimaryHashKey.compareTo(this.unindexedKeys.peekFirst().primaryHashKey) > 0) {
            this.unindexedKeys.removeFirst();
        }

        // Create an Attribute Iterator that will give all Primary Hashes from the one we are searching.
        return null;
//        return segment.attributeIterator(nextPrimaryHashKey, AttributeCalculator.MAX_PRIMARY_HASH_KEY, fetchTimeout)
//                .thenCompose(primaryHashIterator -> primaryHashIterator.compose(this::getAllBuckets, this.executor))
//                .thenCompose(attributeIterator ->{
//                    //For each result, load up all required secondary hashes to construct the buckets.
//
//
//                    // 4. For each bucket returned above, include all Buckets/hashes from the ContainerKeyIndex which are equal to or
//                    // below it. (this is very similar to the AttributeMixer - maybe reuse that methodology).
//
//                    // 5. Return when there is no more stuff to load or when we have loaded the minimum number of buckets.
//                });
    }

    //endregion

    private UUID getNextPrimaryHash() {
        val state = this.state.get();
        return state == null
                ? AttributeCalculator.MIN_PRIMARY_HASH_KEY
                : this.attributeCalculator.getPrimaryHashAttributeKey(state.getLastPrimaryHash());

    }

    private ArrayDeque<UnindexedKey> getUnindexedKeys(List<HashedArray> unindexedKeyHashes, KeyHasher hasher) {
        return unindexedKeyHashes.stream()
                .map(hasher::wrap)
                .map(hash -> new UnindexedKey(this.attributeCalculator.getPrimaryHashAttributeKey(hash.subSegment(0, AttributeCalculator.PRIMARY_HASH_LENGTH)), hash))
                .sorted(Comparator.comparing(u -> u.primaryHashKey))
                .collect(Collectors.toCollection(ArrayDeque::new));
    }

    @RequiredArgsConstructor
    private static class UnindexedKey {
        private final UUID primaryHashKey;
        private final KeyHash hash;

        @Override
        public String toString() {
            return String.format("%s: %s", this.primaryHashKey, this.hash);
        }
    }
}
