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

import com.google.common.annotations.Beta;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
<<<<<<< HEAD
<<<<<<< HEAD
import io.pravega.common.util.BufferView;
=======
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
import io.pravega.common.util.BufferView;
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.Data;

/**
 * Defines an index that maintains a Table Segment's Keys in lexicographic bitwise order.
 */
@Beta
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
<<<<<<< HEAD
<<<<<<< HEAD
    void includeTailCache(Map<? extends BufferView, CacheBucketOffset> tailUpdates);
=======
    void includeTailCache(Map<? extends ArrayView, CacheBucketOffset> tailUpdates);
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
    void includeTailCache(Map<? extends BufferView, CacheBucketOffset> tailUpdates);
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)

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
     *
     * @param range        Iterator range.
     * @param fetchTimeout Timeout for each fetch triggered by {@link AsyncIterator#getNext()}.
     * @return An {@link AsyncIterator} that can be used to iterate keys.
     */
<<<<<<< HEAD
<<<<<<< HEAD
    AsyncIterator<List<BufferView>> iterator(IteratorRange range, Duration fetchTimeout);
=======
    AsyncIterator<List<ArrayView>> iterator(IteratorRange range, Duration fetchTimeout);
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
    AsyncIterator<List<BufferView>> iterator(IteratorRange range, Duration fetchTimeout);
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)

    /**
     * Generates a {@link IteratorRange} that can be used as argument to {@link #iterator} from the given input.
     *
     * @param fromKeyExclusive The lower bound of the iteration (exclusive). If this iteration is resumed (from a previously
     *                         interrupted one), should be the last key that was returned.
     * @param prefix           The prefix of all keys returned.
     * @return An {@link IteratorRange}.
     */
<<<<<<< HEAD
<<<<<<< HEAD
    IteratorRange getIteratorRange(@Nullable BufferView fromKeyExclusive, @Nullable BufferView prefix);
=======
    IteratorRange getIteratorRange(@Nullable ArrayView fromKeyExclusive, @Nullable ArrayView prefix);
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
    IteratorRange getIteratorRange(@Nullable BufferView fromKeyExclusive, @Nullable BufferView prefix);
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)

    /**
     * Arguments for {@link #iterator}.
     */
    @Data
    class IteratorRange {
        /**
         * An {@link ArrayView} representing the lower bound of the iteration (exclusive). All returned keys will be larger
         * than this one. If null, the iteration will start from the smallest key in the segment.
         */
        private final ArrayView from;
        /**
         * An {@link ArrayView representing the upper bound of the iteration (exclusive). All returned keys will be smaller
         * than this one. If null, the iteration will proceed through the largest key in the segment.
         */
        private final ArrayView to;
    }

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
<<<<<<< HEAD
<<<<<<< HEAD
            public void includeTailCache(Map<? extends BufferView, CacheBucketOffset> tailUpdates) {
=======
            public void includeTailCache(Map<? extends ArrayView, CacheBucketOffset> tailUpdates) {
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
            public void includeTailCache(Map<? extends BufferView, CacheBucketOffset> tailUpdates) {
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
                // This method intentionally left blank.
            }

            @Override
            public void updateSegmentIndexOffset(long offset) {
                // This method intentionally left blank.
            }

            @Override
<<<<<<< HEAD
<<<<<<< HEAD
            public AsyncIterator<List<BufferView>> iterator(IteratorRange range, Duration fetchTimeout) {
=======
            public AsyncIterator<List<ArrayView>> iterator(IteratorRange range, Duration fetchTimeout) {
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
            public AsyncIterator<List<BufferView>> iterator(IteratorRange range, Duration fetchTimeout) {
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
                return () -> CompletableFuture.completedFuture(null);
            }

            @Override
<<<<<<< HEAD
<<<<<<< HEAD
            public IteratorRange getIteratorRange(@Nullable BufferView fromKeyExclusive, @Nullable BufferView prefix) {
=======
            public IteratorRange getIteratorRange(@Nullable ArrayView fromKeyExclusive, @Nullable ArrayView prefix) {
>>>>>>> Issue 4656: (KeyValue Tables) Sorted Table Segments (#4763)
=======
            public IteratorRange getIteratorRange(@Nullable BufferView fromKeyExclusive, @Nullable BufferView prefix) {
>>>>>>> Issue 4569: (Key-Value Tables) Merge latest master with feature-key-value-tables (#4892)
                return new IteratorRange(null, null);
            }
        };
    }
}
