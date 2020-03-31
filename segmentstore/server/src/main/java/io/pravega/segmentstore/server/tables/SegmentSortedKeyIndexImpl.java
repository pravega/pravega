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
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Implementation for {@link SegmentSortedKeyIndex}.
 */
@RequiredArgsConstructor
public class SegmentSortedKeyIndexImpl implements SegmentSortedKeyIndex {
    @NonNull
    private final UpdateNodes updateNodes;
    @NonNull
    private final DeleteNodes deleteNodes;
    @NonNull
    private final GetNodes getNodes;

    @Override
    public CompletableFuture<Void> persistUpdate(Collection<BucketUpdate> bucketUpdates, Duration timeout) {
        return null;
    }

    @Override
    public void includeTailUpdate(TableKeyBatch batch, long batchSegmentOffset) {

    }

    @Override
    public void includeTailCache(Map<ArrayView, CacheBucketOffset> tailUpdates) {

    }

    @Override
    public void updateSegmentIndexOffset(long offset) {

    }

    @Override
    public AsyncIterator<List<ArrayView>> iterator(ArrayView prefix, Duration fetchTimeout) {
        return null;
    }

    @FunctionalInterface
    public interface UpdateNodes {
        CompletableFuture<?> apply(List<TableEntry> entries, Duration timeout);
    }

    @FunctionalInterface
    public interface DeleteNodes {
        CompletableFuture<?> apply(Collection<TableKey> keys, Duration timeout);
    }

    @FunctionalInterface
    public interface GetNodes {
        CompletableFuture<List<TableEntry>> apply(List<ArrayView> keys, Duration timeout);
    }
}
