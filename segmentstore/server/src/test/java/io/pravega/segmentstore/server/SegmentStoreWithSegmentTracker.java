/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.contracts.tables.TableStore;
import lombok.AccessLevel;
import lombok.Getter;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A wrapper class to {@link StreamSegmentStore} and {@link TableStore} to track the segments being created or deleted
 * using their methods.
 * The set of segments obtained in here is used in Data Recovery integration test to wait for those segments to be
 * flushed to the storage, so that segments recovery from the storage can be performed.
 */
public class SegmentStoreWithSegmentTracker implements StreamSegmentStore, TableStore {
    private final StreamSegmentStore streamSegmentStore;
    private final TableStore tableStore;

    @Getter(AccessLevel.PUBLIC)
    private final ConcurrentHashMap<String, Boolean> segments;

    /**
     * Creates an instance of the SegmentStoreWithSegmentTracker class.
     * @param streamSegmentStore    The instance of {@link StreamSegmentStore} to direct method calls for various operations.
     * @param tableStore            The instance of {@link TableStore} to direct method calls for various operations.
     */
    public SegmentStoreWithSegmentTracker(StreamSegmentStore streamSegmentStore, TableStore tableStore) {
        this.streamSegmentStore = streamSegmentStore;
        this.tableStore = tableStore;
        this.segments = new ConcurrentHashMap<>();
    }

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, BufferView data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        return this.streamSegmentStore.append(streamSegmentName, data, attributeUpdates, timeout);
    }

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, long offset, BufferView data, Collection<AttributeUpdate> attributeUpdates,
                                          Duration timeout) {
        return this.streamSegmentStore.append(streamSegmentName, offset, data, attributeUpdates, timeout);
    }

    @Override
    public CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        return this.streamSegmentStore.updateAttributes(streamSegmentName, attributeUpdates, timeout);
    }

    @Override
    public CompletableFuture<Map<UUID, Long>> getAttributes(String streamSegmentName, Collection<UUID> attributeIds, boolean cache, Duration timeout) {
        return this.streamSegmentStore.getAttributes(streamSegmentName, attributeIds, cache, timeout);
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        return this.streamSegmentStore.read(streamSegmentName, offset, maxLength, timeout);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return this.streamSegmentStore.getStreamSegmentInfo(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        // Add the segmentName to the set
        return this.streamSegmentStore.createStreamSegment(streamSegmentName, attributes, timeout)
                .thenRun(() -> segments.put(streamSegmentName, true));
    }

    @Override
    public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetStreamSegment, String sourceStreamSegment, Duration timeout) {
        return this.streamSegmentStore.mergeStreamSegment(targetStreamSegment, sourceStreamSegment, timeout);
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        return this.streamSegmentStore.sealStreamSegment(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        // Remove the segmentName from the set
        return this.streamSegmentStore.deleteStreamSegment(streamSegmentName, timeout)
                .thenRun(() -> segments.remove(streamSegmentName));
    }

    @Override
    public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
        return this.streamSegmentStore.truncateStreamSegment(streamSegmentName, offset, timeout);
    }

    @Override
    public CompletableFuture<Void> createSegment(String segmentName, Duration timeout) {
        // Add the segmentName to the set
        return this.tableStore.createSegment(segmentName, timeout).thenRun(() -> segments.put(segmentName, true));
    }

    @Override
    public CompletableFuture<Void> createSegment(String segmentName, boolean sorted, Duration timeout) {
        // Add the segmentName to the set
        return this.tableStore.createSegment(segmentName, sorted, timeout).thenRun(() -> segments.put(segmentName, true));
    }

    @Override
    public CompletableFuture<Void> deleteSegment(String segmentName, boolean mustBeEmpty, Duration timeout) {
        // Remove the segmentName from the set
        return this.tableStore.deleteSegment(segmentName, mustBeEmpty, timeout).thenRun(() -> segments.remove(segmentName));
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
    public CompletableFuture<List<Long>> put(String segmentName, List<TableEntry> entries, long tableSegmentOffset, Duration timeout) {
        return this.tableStore.put(segmentName, entries, tableSegmentOffset, timeout);
    }

    @Override
    public CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, Duration timeout) {
        return this.tableStore.remove(segmentName, keys, timeout);
    }

    @Override
    public CompletableFuture<Void> remove(String segmentName, Collection<TableKey> keys, long tableSegmentOffset, Duration timeout) {
        return this.tableStore.remove(segmentName, keys, tableSegmentOffset, timeout);
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

    @Override
    public CompletableFuture<AsyncIterator<IteratorItem<TableEntry>>> entryDeltaIterator(String segmentName, long fromPosition, Duration fetchTimeout) {
        return this.tableStore.entryDeltaIterator(segmentName, fromPosition, fetchTimeout);
    }
}
