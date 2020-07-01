/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

import io.pravega.common.util.BufferView;
import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A wrapper class to StreamSegmentStore to track the segments being created or deleted.
 */
public class StreamSegmentStoreWrapper implements StreamSegmentStore {

    private final StreamSegmentStore streamSegmentStore;
    private HashSet<String> segments;

    public StreamSegmentStoreWrapper(StreamSegmentStore streamSegmentStore) {
        this.streamSegmentStore = streamSegmentStore;
        this.segments = new HashSet<>();
    }

    public HashSet<String> getSegments() {
        return this.segments;
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
        segments.add(streamSegmentName); // Add the segmentName to the set
        return this.streamSegmentStore.createStreamSegment(streamSegmentName, attributes, timeout);
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
        if (segments.contains(streamSegmentName)) { // Remove the segmentName from the set
            segments.remove(streamSegmentName);
        }
        return this.streamSegmentStore.deleteStreamSegment(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
        return this.streamSegmentStore.truncateStreamSegment(streamSegmentName, offset, timeout);
    }
}
