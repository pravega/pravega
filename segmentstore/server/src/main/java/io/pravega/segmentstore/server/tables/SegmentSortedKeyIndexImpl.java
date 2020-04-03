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
import io.pravega.common.util.ByteArrayComparator;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.val;

/**
 * Implementation for {@link SegmentSortedKeyIndex}.
 */
@RequiredArgsConstructor
class SegmentSortedKeyIndexImpl implements SegmentSortedKeyIndex {
    //region Members

    private static final ByteArrayComparator KEY_COMPARATOR = new ByteArrayComparator();
    @NonNull
    private final UpdateNodes updateNodes;
    @NonNull
    private final DeleteNodes deleteNodes;
    @NonNull
    private final GetNodes getNodes;
    @GuardedBy("tailKeys")
    private final TreeMap<ArrayView, CacheBucketOffset> tailKeys = new TreeMap<>(KEY_COMPARATOR::compare);

    //endregion

    //region SegmentSortedKeyIndex Implementation

    @Override
    public CompletableFuture<Void> persistUpdate(Collection<BucketUpdate> bucketUpdates, Duration timeout) {
        return null;
    }

    @Override
    public void includeTailUpdate(TableKeyBatch batch, long batchSegmentOffset) {
        synchronized (this.tailKeys) {
            batch.getItems().forEach(item -> this.tailKeys.put(
                    item.getKey().getKey(),
                    new CacheBucketOffset(batchSegmentOffset + item.getOffset(), batch.isRemoval())));
        }
    }

    @Override
    public void includeTailCache(Map<ArrayView, CacheBucketOffset> tailUpdates) {
        synchronized (this.tailKeys) {
            this.tailKeys.putAll(tailUpdates);
        }
    }

    @Override
    public void updateSegmentIndexOffset(long offset) {
        synchronized (this.tailKeys) {
            if (offset < 0) {
                // Segment evicted.
                this.tailKeys.clear();
            } else {
                // Indexed up to an offset. It's safe to remove any tail entries below the offset.
                val toRemove = this.tailKeys.entrySet().stream()
                        .filter(e -> e.getValue().getSegmentOffset() <= offset)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
                toRemove.forEach(this.tailKeys::remove);
            }
        }
    }

    @Override
    public AsyncIterator<List<ArrayView>> iterator(ArrayView prefix, Duration fetchTimeout) {
        return null;
    }

    //endregion

    //region Functional Interfaces

    @FunctionalInterface
    interface UpdateNodes {
        CompletableFuture<?> apply(List<TableEntry> entries, Duration timeout);
    }

    @FunctionalInterface
    interface DeleteNodes {
        CompletableFuture<?> apply(Collection<TableKey> keys, Duration timeout);
    }

    @FunctionalInterface
    interface GetNodes {
        CompletableFuture<List<TableEntry>> apply(List<ArrayView> keys, Duration timeout);
    }

    //endregion
}
