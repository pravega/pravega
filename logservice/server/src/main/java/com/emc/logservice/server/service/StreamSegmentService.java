package com.emc.logservice.server.service;

import com.emc.logservice.common.*;
import com.emc.logservice.contracts.*;
import com.emc.logservice.server.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.emc.logservice.common.LoggerHelpers.traceLeave;

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
        Exceptions.throwIfNull(segmentContainerRegistry, "segmentContainerRegistry");
        Exceptions.throwIfNull(segmentToContainerMapper, "segmentToContainerMapper");

        this.segmentContainerRegistry = segmentContainerRegistry;
        this.segmentToContainerMapper = segmentToContainerMapper;
    }

    //endregion

    //region StreamSegmentStore Implementation

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, byte[] data, AppendContext appendContext, Duration timeout) {
        int traceId = LoggerHelpers.traceEnter(log, "append", streamSegmentName, data.length, appendContext, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName).thenCompose(container -> container.append(streamSegmentName, data, appendContext, timeout)),
                r -> traceLeave(log, "append", traceId, r));
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        int traceId = LoggerHelpers.traceEnter(log, "read", streamSegmentName, offset, maxLength, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName)
                        .thenCompose(container -> container.read(streamSegmentName, offset, maxLength, timeout)),
                r -> traceLeave(log, "read", traceId, r));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        int traceId = LoggerHelpers.traceEnter(log, "getStreamSegmentInfo", streamSegmentName, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName)
                        .thenCompose(container -> container.getStreamSegmentInfo(streamSegmentName, timeout)),
                r -> traceLeave(log, "getStreamSegmentInfo", traceId, r));
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Duration timeout) {
        int traceId = LoggerHelpers.traceEnter(log, "createStreamSegment", streamSegmentName, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName)
                        .thenCompose(container -> container.createStreamSegment(streamSegmentName, timeout)),
                r -> traceLeave(log, "createStreamSegment", traceId, r));
    }

    @Override
    public CompletableFuture<String> createBatch(String parentStreamSegmentName, Duration timeout) {
        int traceId = LoggerHelpers.traceEnter(log, "createBatch", parentStreamSegmentName, timeout);
        return withCompletion(
                () -> getContainer(parentStreamSegmentName)
                        .thenCompose(container -> container.createBatch(parentStreamSegmentName, timeout)),
                r -> traceLeave(log, "createBatch", traceId, r));
    }

    @Override
    public CompletableFuture<Long> mergeBatch(String batchName, Duration timeout) {
        int traceId = LoggerHelpers.traceEnter(log, "mergeBatch", batchName, timeout);
        return withCompletion(
                () -> getContainer(batchName)
                        .thenCompose(container -> container.mergeBatch(batchName, timeout)),
                r -> traceLeave(log, "mergeBatch", traceId, r));
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        int traceId = LoggerHelpers.traceEnter(log, "sealStreamSegment", streamSegmentName, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName)
                        .thenCompose(container -> container.sealStreamSegment(streamSegmentName, timeout)),
                r -> traceLeave(log, "deleteStreamSegment", traceId, r));
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        int traceId = LoggerHelpers.traceEnter(log, "deleteStreamSegment", streamSegmentName, timeout);
        return withCompletion(
                () -> getContainer(streamSegmentName)
                        .thenCompose(container -> container.deleteStreamSegment(streamSegmentName, timeout)),
                r -> traceLeave(log, "deleteStreamSegment", traceId, r));
    }

    @Override
    public CompletableFuture<AppendContext> getLastAppendContext(String streamSegmentName, UUID clientId) {
        int traceId = LoggerHelpers.traceEnter(log, "getLastAppendContext", streamSegmentName, clientId);
        return withCompletion(
                () -> getContainer(streamSegmentName)
                        .thenCompose(container -> container.getLastAppendContext(streamSegmentName, clientId)),
                r -> traceLeave(log, "getLastAppendContext", traceId, r));
    }

    private <T> CompletableFuture<T> withCompletion(Supplier<CompletableFuture<T>> supplier, Consumer<T> leaveCallback) {
        CompletableFuture<T> resultFuture = supplier.get();
        resultFuture.thenAccept(r -> CallbackHelpers.invokeSafely(leaveCallback, r, null));
        return resultFuture;
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
