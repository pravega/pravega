/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts.tables;

import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class to TableStore to track the segments being created or deleted.
 */
public class TableStoreWrapper implements TableStore {
    private final TableStore tableStore;
    private HashSet<String> segments;

    public TableStoreWrapper(TableStore tableStore) {
        this.tableStore = tableStore;
        this.segments = new HashSet<>();
    }

    public HashSet<String> getSegments() {
        return this.segments;
    }

    @Override
    public CompletableFuture<Void> createSegment(String segmentName, Duration timeout) {
        this.segments.add(segmentName); // Add the segmentName to the set
        return this.tableStore.createSegment(segmentName, timeout);
    }

    @Override
    public CompletableFuture<Void> createSegment(String segmentName, boolean sorted, Duration timeout) {
        this.segments.add(segmentName); // Add the segmentName to the set
        return this.tableStore.createSegment(segmentName, sorted, timeout);
    }

    @Override
    public CompletableFuture<Void> deleteSegment(String segmentName, boolean mustBeEmpty, Duration timeout) {
        if (this.segments.contains(segmentName)) { // Remove the segmentName from the set
            this.segments.remove(segmentName);
        }
        return this.tableStore.deleteSegment(segmentName, mustBeEmpty, timeout);
    }

    @Override
    public CompletableFuture<Void> merge(String targetSegmentName, String sourceSegmentName, Duration timeout) {
        return this.tableStore.merge(targetSegmentName, sourceSegmentName, timeout);
    }

    @Override
    public CompletableFuture<Void> seal(String segmentName, Duration timeout) {
        return this.tableStore.seal(segmentName, timeout);
    }

    @Override
    public CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, Duration timeout) {
        return this.tableStore.put(segmentName, entries, timeout);
    }

    @Override
    public CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, Duration timeout) {
        return this.tableStore.remove(segmentName, keys, timeout);
    }

    @Override
    public CompletableFuture<List<TableEntry>> get(String segmentName, List<BufferView> keys, Duration timeout) {
        return this.tableStore.get(segmentName, keys, timeout);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableKey>>> keyIterator(String segmentName, IteratorArgs args) {
        return this.tableStore.keyIterator(segmentName, args);
    }

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryIterator(String segmentName, IteratorArgs args) {
        return this.tableStore.entryIterator(segmentName, args);
    }
}
