/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.store;

import com.google.common.base.Preconditions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.ContainerNotFoundException;
import io.pravega.segmentstore.contracts.DirectSegmentAccess;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerRegistry;
import io.pravega.shared.segment.SegmentToContainerMapper;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

/**
 * This is the Log/StreamSegment Service, that puts together everything and is what should be exposed to the outside.
 */
@Slf4j
public class StreamSegmentService implements StreamSegmentStore {
    //region Members

    private final SegmentContainerRegistry segmentContainerRegistry;
    private final SegmentToContainerMapper segmentToContainerMapper;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentService class.
     *
     * @param segmentContainerRegistry The SegmentContainerRegistry to route requests to.
     * @param segmentToContainerMapper The SegmentToContainerMapper to use to map StreamSegments to Containers.
     */
    public StreamSegmentService(SegmentContainerRegistry segmentContainerRegistry, SegmentToContainerMapper segmentToContainerMapper) {
        this.segmentContainerRegistry = Preconditions.checkNotNull(segmentContainerRegistry, "segmentContainerRegistry");
        this.segmentToContainerMapper = Preconditions.checkNotNull(segmentToContainerMapper, "segmentToContainerMapper");
    }

    //endregion

    //region StreamSegmentStore Implementation

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.append(streamSegmentName, data, attributeUpdates, timeout),
                "append", streamSegmentName, data.length, attributeUpdates);
    }

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, long offset, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.append(streamSegmentName, offset, data, attributeUpdates, timeout),
                "appendWithOffset", streamSegmentName, offset, data.length, attributeUpdates);
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
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, boolean waitForPendingOps, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.getStreamSegmentInfo(streamSegmentName, waitForPendingOps, timeout),
                "getStreamSegmentInfo", streamSegmentName, waitForPendingOps);
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.createStreamSegment(streamSegmentName, attributes, timeout),
                "createStreamSegment", streamSegmentName, attributes);
    }

    @Override
    public CompletableFuture<SegmentProperties> mergeStreamSegment(String targetStreamSegment, String sourceStreamSegment, Duration timeout) {
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

    @Override
    public CompletableFuture<DirectSegmentAccess> forSegment(String streamSegmentName, Duration timeout) {
        return invoke(
                streamSegmentName,
                container -> container.forSegment(streamSegmentName, timeout),
                "forSegment", streamSegmentName);
    }

    //endregion

    //region Helpers

    /**
     * Executes the given Function on the SegmentContainer that the given Segment maps to.
     *
     * @param streamSegmentName The name of the StreamSegment to fetch the Container for.
     * @param toInvoke          A Function that will be invoked on the Container.
     * @param methodName        The name of the calling method (for logging purposes).
     * @param logArgs           (Optional) A vararg array of items to be logged.
     * @param <T>               Resulting type.
     * @return Either the result of toInvoke or a CompletableFuture completed exceptionally with a ContainerNotFoundException
     * in case the SegmentContainer that the Segment maps to does not exist in this StreamSegmentService.
     */
    private <T> CompletableFuture<T> invoke(String streamSegmentName, Function<SegmentContainer, CompletableFuture<T>> toInvoke,
                                            String methodName, Object... logArgs) {
        long traceId = LoggerHelpers.traceEnter(log, methodName, logArgs);
        SegmentContainer container;
        try {
            int containerId = this.segmentToContainerMapper.getContainerId(streamSegmentName);
            container = this.segmentContainerRegistry.getContainer(containerId);
        } catch (ContainerNotFoundException ex) {
            return Futures.failedFuture(ex);
        }

        CompletableFuture<T> resultFuture = toInvoke.apply(container);
        if (log.isTraceEnabled()) {
            resultFuture.thenAccept(r -> LoggerHelpers.traceLeave(log, methodName, traceId, r));
        }

        return resultFuture;
    }

    //endregion
}