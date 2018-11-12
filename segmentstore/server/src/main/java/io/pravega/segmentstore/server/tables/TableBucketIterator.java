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

import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AsyncIterator;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.val;

/**
 * Iterates through {@link TableBucket}s in a Segment.
 */
class TableBucketIterator implements AsyncIterator<IteratorItem<TableBucket>> {
    private static final int MIN_ITERATION_BATCH = 10; // Should we accept this via the input?
    private final DirectSegmentAccess segment;
    private final AtomicReference<IteratorState> previousState;
    private final ScheduledExecutorService executor;
    private final Duration fetchTimeout;
    private final ArrayDeque<UUID> unindexedKeyHashes;

    TableBucketIterator(@NonNull DirectSegmentAccess segment, @NonNull List<UUID> unindexedKeyHashes,
                        IteratorState previousState, @NonNull ScheduledExecutorService executor, @NonNull Duration fetchTimeout) {
        this.segment = segment;
        this.previousState = new AtomicReference<>(previousState == null ? IteratorState.START : previousState);
        this.executor = executor;
        this.fetchTimeout = fetchTimeout;
        this.unindexedKeyHashes = getUnindexedKeyHashes(unindexedKeyHashes, this.previousState.get().getKeyHash());
    }

    //region AsyncIterator Implementation

    @Override
    public CompletableFuture<IteratorItem<TableBucket>> getNext() {
        // Calculate the current iteration's state, which is the one immediately after the one we have
        IteratorState state = this.previousState.get();
        if (state.isEnd()) {
            // We are done.
            return null;
        }

        UUID firstKeyHash = getNextKeyHash(state);
        assert firstKeyHash != null && IteratorState.isValid(firstKeyHash) : "getNextKeyHash generated a null or invalid successor.";

        // Create an Attribute Iterator that will give all Key Hashes from the one we are searching, then loop through
        // it until we have enough TableBuckets to return or until we reach the end.
        val buckets = new ArrayList<TableBucket>();
        return this.segment
                .attributeIterator(firstKeyHash, IteratorState.MAX_HASH, this.fetchTimeout)
                .thenCompose(hashes -> processHashIterator(hashes, buckets))
                .thenApply(v -> {
                    IteratorState newState = buckets.isEmpty()
                            ? IteratorState.END
                            : new IteratorState(buckets.get(buckets.size() - 1).getHash());
                    val result = new IteratorItem<TableBucket>(newState.serialize(), buckets);
                    if (!this.previousState.compareAndSet(state, newState)) {
                        throw new IllegalStateException("Concurrent call to getNext() detected.");
                    }

                    return result;
                });
    }

    //endregion

    private CompletableFuture<Void> processHashIterator(AsyncIterator<List<Map.Entry<UUID, Long>>> hashIterator, List<TableBucket> buckets) {
        AtomicBoolean canContinue = new AtomicBoolean(true);
        return Futures.loop(
                canContinue::get,
                () -> hashIterator.getNext().thenApplyAsync(attributes -> process(attributes, buckets), this.executor),
                canContinue::set,
                this.executor);
    }

    private boolean process(List<Map.Entry<UUID, Long>> attributes, List<TableBucket> buckets) {
        if (attributes == null) {
            // Nothing more to do.
            return false;
        }

        // Transform every eligible Attribute into a TableBucket and add it to the result.
        for (val a : attributes) {
            if (IteratorState.isValid(a.getKey()) && a.getValue() != Attributes.NULL_ATTRIBUTE_VALUE) {
                buckets.add(new TableBucket(a.getKey(), a.getValue()));

                // TODO For each bucket returned above, include all Buckets/hashes from the ContainerKeyIndex which are equal to or
                // below it. (this is very similar to the AttributeMixer - maybe reuse that methodology).
                //        while (!this.unindexedKeyHashes.isEmpty() && firstKeyHash.compareTo(this.unindexedKeyHashes.peekFirst()) > 0) {
                //            this.unindexedKeyHashes.removeFirst();
                //        }

                // 5. Return when there is no more stuff to load or when we have loaded the minimum number of buckets.
            }
        }

        // Once we reached our minimum number of elements we can stop.
        return buckets.size() < MIN_ITERATION_BATCH;
    }

    private IteratorItem<TableBucket> createResult(List<TableBucket> buckets) {
        IteratorState state = buckets.isEmpty() ? IteratorState.END : new IteratorState(buckets.get(buckets.size() - 1).getHash());
        return new IteratorItem<>(state.serialize(), buckets);
    }

    /**
     * Generates a new Key Hash that is immediately after the one from the given IteratorState. We define Key Hash H2 to
     * be immediately after Key Hash h1 if there doesn't exist Key Hash H3 such that H1&lt;H3&lt;H2. The ordering is
     * performed using {@link UUID#compareTo}.
     *
     * @return The successor Key Hash, or null if no more successors are available (if {@link IteratorState#isEnd} returns true).
     */
    private UUID getNextKeyHash(IteratorState state) {
        if (state.isEnd()) {
            return null;
        }

        long msb = state.getKeyHash().getMostSignificantBits();
        long lsb = state.getKeyHash().getLeastSignificantBits();
        if (lsb == Long.MAX_VALUE) {
            msb++; // This won't overflow since we've checked that state is not end (i.e., id != MAX).
            lsb = Long.MIN_VALUE;
        } else {
            lsb++;
        }

        return new UUID(msb, lsb);
    }

    private ArrayDeque<UUID> getUnindexedKeyHashes(List<UUID> unindexedKeyHashes, UUID firstHash) {
        // Filter out the Hashes which are below our first hash, then sort them.
        return unindexedKeyHashes.stream()
                                 .filter(id -> id.compareTo(firstHash) > 0)
                                 .sorted().collect(Collectors.toCollection(ArrayDeque::new));
    }
}
