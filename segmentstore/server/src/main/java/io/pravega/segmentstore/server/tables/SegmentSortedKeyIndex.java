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

import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an index that maintains a Table Segment's Keys in lexicographic bitwise order.
 */
interface SegmentSortedKeyIndex {
    /**
     * Include and persist updates that have been included in a Table Segment Index.
     *
     * @param bucketUpdates A Collection of {@link BucketUpdate} instances that reflect what keys have been added and/or
     *                      removed.
     * @param timeout       Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation has completed.
     */
    CompletableFuture<Void> persistUpdate(Collection<BucketUpdate> bucketUpdates, Duration timeout);

    /**
     * Includes the given {@link TableKeyBatch} which contains Keys that have recently been updated or removed, but not
     * yet indexed. These will be stored in a memory data structure until {@link #updateSegmentIndexOffset} will be invoked
     * with an offset that exceeds their offset.
     *
     * @param batch              The {@link TableKeyBatch} to include.
     * @param batchSegmentOffset The offset of the {@link TableKeyBatch}.
     */
    void includeTailUpdate(TableKeyBatch batch, long batchSegmentOffset);

    /**
     * Includes the given collection of Keys to {@link CacheBucketOffset}s that have recently been pre-indexed as part
     * of a Table Segment recovery process. These are updates that have not yet been persisted using {@link #persistUpdate}
     * and will be stored in a memory data structure until {@link #updateSegmentIndexOffset} will be invoked with an offset
     * that exceeds their offset.
     *
     * @param tailUpdates The updates to include.
     */
    void includeTailCache(Map<ArrayView, CacheBucketOffset> tailUpdates);

    /**
     * Notifies that the Table Segment has indexed and durably persisted all updates up to and including the given offset.
     * All in-memory updates included via {@link #includeTailCache} or {@link #includeTailUpdate} which have offsets
     * prior to the given one will be deleted from memory (as their data is now persisted).
     *
     * @param offset The offset.
     */
    void updateSegmentIndexOffset(long offset);

    /**
     * Creates a new Key Iterator for all Keys beginning with the given prefix.
     * TODO: IteratorState???
     *
     * @param prefix       An {@link ArrayView} representing the prefix. If empty, all keys will iterated on.
     * @param fetchTimeout Timeout for each fetch triggered by {@link AsyncIterator#getNext()}.
     * @return An {@link AsyncIterator} that can be used to iterate keys.
     */
    AsyncIterator<List<ArrayView>> iterator(ArrayView prefix, Duration fetchTimeout);

    /**
     * Creates a {@link SegmentSortedKeyIndex} that does nothing.
     *
     * @return A no-op {@link SegmentSortedKeyIndex}.
     */
    static SegmentSortedKeyIndex noop() {
        return new SegmentSortedKeyIndex() {
            @Override
            public CompletableFuture<Void> persistUpdate(Collection<BucketUpdate> bucketUpdates, Duration timeout) {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public void includeTailUpdate(TableKeyBatch batch, long batchSegmentOffset) {
                // This method intentionally left blank.
            }

            @Override
            public void includeTailCache(Map<ArrayView, CacheBucketOffset> tailUpdates) {
                // This method intentionally left blank.
            }

            @Override
            public void updateSegmentIndexOffset(long offset) {
                // This method intentionally left blank.
            }

            @Override
            public AsyncIterator<List<ArrayView>> iterator(ArrayView prefix, Duration fetchTimeout) {
                return () -> null;
            }
        };
    }
}
