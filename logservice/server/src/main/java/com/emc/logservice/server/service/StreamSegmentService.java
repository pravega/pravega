package com.emc.logservice.server.service;

import com.emc.logservice.common.FutureHelpers;
import com.emc.logservice.contracts.*;
import com.emc.logservice.server.*;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * This is the Log/StreamSegment Service, that puts together everything and is what should be exposed to the outside.
 */
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
        if (segmentContainerRegistry == null) {
            throw new NullPointerException("segmentContainerRegistry");
        }
        if (segmentToContainerMapper == null) {
            throw new NullPointerException("segmentToContainerMapper");
        }

        this.segmentContainerRegistry = segmentContainerRegistry;
        this.segmentToContainerMapper = segmentToContainerMapper;
    }

    //endregion

    //region StreamSegmentStore Implementation

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, byte[] data, AppendContext appendContext, Duration timeout) {
        CompletableFuture<Long> result = getContainer(streamSegmentName).thenCompose(container -> container.append(streamSegmentName, data, appendContext, timeout));
        //result.whenComplete((r, ex) -> System.out.println(String.format("Append to %s (%d bytes) complete: Result %d, Error %s.", streamSegmentName, data.length, r, ex)));
        return result;
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        return getContainer(streamSegmentName).thenCompose(container -> container.read(streamSegmentName, offset, maxLength, timeout));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        return getContainer(streamSegmentName).thenCompose(container -> container.getStreamSegmentInfo(streamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Duration timeout) {
        return getContainer(streamSegmentName).thenCompose(container -> container.createStreamSegment(streamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<String> createBatch(String parentStreamSegmentName, Duration timeout) {
        return getContainer(parentStreamSegmentName).thenCompose(container -> container.createBatch(parentStreamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<Long> mergeBatch(String batchName, Duration timeout) {
        return getContainer(batchName).thenCompose(container -> container.mergeBatch(batchName, timeout));
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        return getContainer(streamSegmentName).thenCompose(container -> container.sealStreamSegment(streamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        return getContainer(streamSegmentName).thenCompose(container -> container.deleteStreamSegment(streamSegmentName, timeout));
    }

    @Override
    public CompletableFuture<AppendContext> getLastAppendContext(String streamSegmentName, UUID clientId) {
        return getContainer(streamSegmentName).thenCompose(container -> container.getLastAppendContext(streamSegmentName, clientId));
    }

    //endregion

    //region Helpers

    private CompletableFuture<SegmentContainer> getContainer(String streamSegmentName) {
        String containerId = this.segmentToContainerMapper.getContainerId(streamSegmentName);
        try {
            return CompletableFuture.completedFuture(this.segmentContainerRegistry.getContainer(containerId));
        }
        catch (ContainerNotFoundException ex) {
            return FutureHelpers.failedFuture(ex);
        }
    }

    //endregion
}
