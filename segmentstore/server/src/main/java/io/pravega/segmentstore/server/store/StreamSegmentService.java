/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.store;

import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.SegmentContainerRegistry;
import io.pravega.shared.segment.SegmentToContainerMapper;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;

/**
 * This is the Log/StreamSegment Service, that puts together everything and is what should be exposed to the outside.
 */
@Slf4j
public class StreamSegmentService extends SegmentContainerCollection implements StreamSegmentStore {

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentService class.
     *
     * @param segmentContainerRegistry The SegmentContainerRegistry to route requests to.
     * @param segmentToContainerMapper The SegmentToContainerMapper to use to map StreamSegments to Containers.
     */
    public StreamSegmentService(SegmentContainerRegistry segmentContainerRegistry, SegmentToContainerMapper segmentToContainerMapper) {
        super(segmentContainerRegistry, segmentToContainerMapper);
    }

    //endregion

    //region StreamSegmentStore Implementation

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, BufferView data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.append(streamSegmentName, data, attributeUpdates, timeout),
                "append", streamSegmentName, data.getLength(), attributeUpdates);
    }

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, long offset, BufferView data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.append(streamSegmentName, offset, data, attributeUpdates, timeout),
                "appendWithOffset", streamSegmentName, offset, data.getLength(), attributeUpdates);
    }

    @Override
    public CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.updateAttributes(streamSegmentName, attributeUpdates, timeout),
                "updateAttributes", streamSegmentName, attributeUpdates);
    }

    @Override
    public CompletableFuture<Map<UUID, Long>> getAttributes(String streamSegmentName, Collection<UUID> attributeIds, boolean cache, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.getAttributes(streamSegmentName, attributeIds, cache, timeout),
                "getAttributes", streamSegmentName, attributeIds);
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.read(streamSegmentName, offset, maxLength, timeout),
                "read", streamSegmentName, offset, maxLength);
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.getStreamSegmentInfo(streamSegmentName, timeout),
                "getStreamSegmentInfo", streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.createStreamSegment(streamSegmentName, attributes, timeout),
                "createStreamSegment", streamSegmentName, attributes);
    }

    @Override
    public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetStreamSegment, String sourceStreamSegment, Duration timeout) {
        return invoke(
                sourceStreamSegment,
                container -> container.mergeStreamSegment(targetStreamSegment, sourceStreamSegment, timeout),
                "mergeTransaction", targetStreamSegment, sourceStreamSegment);
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.sealStreamSegment(streamSegmentName, timeout),
                "sealStreamSegment", streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.deleteStreamSegment(streamSegmentName, timeout),
                "deleteStreamSegment", streamSegmentName);
    }

    @Override
    public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.truncateStreamSegment(streamSegmentName, offset, timeout),
                "truncateStreamSegment", streamSegmentName);
    }

    //endregion
}
