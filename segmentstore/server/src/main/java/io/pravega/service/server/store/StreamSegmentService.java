/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.server.store;

import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.function.CallbackHelpers;
import io.pravega.common.segment.SegmentToContainerMapper;
import io.pravega.service.contracts.AttributeUpdate;
import io.pravega.service.contracts.ContainerNotFoundException;
import io.pravega.service.contracts.ReadResult;
import io.pravega.service.contracts.SegmentProperties;
import io.pravega.service.contracts.StreamSegmentStore;
import io.pravega.service.server.SegmentContainer;
import io.pravega.service.server.SegmentContainerRegistry;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import lombok.extern.slf4j.Slf4j;

import static io.pravega.common.LoggerHelpers.traceLeave;

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
     * @throws NullPointerException If segmentContainerRegistry is null.
     * @throws NullPointerException If segmentToContainerMapper is null.
     */
    public StreamSegmentService(SegmentContainerRegistry segmentContainerRegistry, SegmentToContainerMapper segmentToContainerMapper) {
        Preconditions.checkNotNull(segmentContainerRegistry, "segmentContainerRegistry");
        Preconditions.checkNotNull(segmentToContainerMapper, "segmentToContainerMapper");

        this.segmentContainerRegistry = segmentContainerRegistry;
        this.segmentToContainerMapper = segmentToContainerMapper;
    }

    //endregion

    //region StreamSegmentStore Implementation

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "append", streamSegmentName, data.length, attributeUpdates, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName).thenCompose(container -> container.append(streamSegmentName, data, attributeUpdates, timeout)),
                r -> traceLeave(log, "append", traceId, r));
    }

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, long offset, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "appendWithOffset", streamSegmentName, offset, data.length, attributeUpdates, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName).thenCompose(container -> container.append(streamSegmentName, offset, data, attributeUpdates, timeout)),
                r -> traceLeave(log, "appendWithOffset", traceId, r));
    }

    @Override
    public CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "updateAttributes", streamSegmentName, attributeUpdates, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName).thenCompose(container -> container.updateAttributes(streamSegmentName, attributeUpdates, timeout)),
                r -> traceLeave(log, "updateAttributes", traceId, r));
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "read", streamSegmentName, offset, maxLength, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName)
                        .thenCompose(container -> container.read(streamSegmentName, offset, maxLength, timeout)),
                r -> traceLeave(log, "read", traceId, r));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, boolean waitForPendingOps, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName)
                        .thenCompose(container -> container.getStreamSegmentInfo(streamSegmentName, waitForPendingOps, timeout)),
                r -> traceLeave(log, "getStreamSegmentInfo", traceId, r));
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "createStreamSegment", streamSegmentName, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName)
                        .thenCompose(container -> container.createStreamSegment(streamSegmentName, attributes, timeout)),
                r -> traceLeave(log, "createStreamSegment", traceId, r));
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStreamSegmentName, UUID transactionId,
                                                       Collection<AttributeUpdate> attributes, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "createTransaction", parentStreamSegmentName, timeout);
        return withCompletion(
                () -> getContainer(parentStreamSegmentName)
                        .thenCompose(container -> container.createTransaction(parentStreamSegmentName, transactionId, attributes, timeout)),
                r -> traceLeave(log, "createTransaction", traceId, r));
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "mergeTransaction", transactionName, timeout);
        return withCompletion(
                () -> getContainer(transactionName)
                        .thenCompose(container -> container.mergeTransaction(transactionName, timeout)),
                r -> traceLeave(log, "mergeTransaction", traceId, r));
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "sealStreamSegment", streamSegmentName, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName)
                        .thenCompose(container -> container.sealStreamSegment(streamSegmentName, timeout)),
                r -> traceLeave(log, "sealStreamSegment", traceId, r));
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        long traceId = LoggerHelpers.traceEnter(log, "deleteStreamSegment", streamSegmentName, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName)
                        .thenCompose(container -> container.deleteStreamSegment(streamSegmentName, timeout)),
                r -> traceLeave(log, "deleteStreamSegment", traceId, r));
    }

    private <T> CompletableFuture<T> withCompletion(Supplier<CompletableFuture<T>> supplier, Consumer<T> leaveCallback) {
        CompletableFuture<T> resultFuture = supplier.get();
        resultFuture.thenAccept(r -> CallbackHelpers.invokeSafely(leaveCallback, r, null));
        return resultFuture;
    }

    //endregion

    //region Helpers

    private CompletableFuture<SegmentContainer> getContainer(String streamSegmentName) {
        int containerId = this.segmentToContainerMapper.getContainerId(streamSegmentName);
        try {
            return CompletableFuture.completedFuture(this.segmentContainerRegistry.getContainer(containerId));
        } catch (ContainerNotFoundException ex) {
            return FutureHelpers.failedFuture(ex);
        }
    }

    //endregion
}