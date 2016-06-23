package com.emc.logservice.server.containers;

import com.emc.logservice.common.*;
import com.emc.logservice.contracts.*;
import com.emc.logservice.server.*;
import com.emc.logservice.server.logs.OperationLog;
import com.emc.logservice.server.logs.PendingAppendsCollection;
import com.emc.logservice.server.logs.operations.*;
import com.emc.logservice.storageabstraction.Storage;
import com.emc.logservice.storageabstraction.StorageFactory;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/**
 * Container for StreamSegments. All StreamSegments that are related (based on a hashing functions) will belong to the
 * same StreamSegmentContainer. Handles all operations that can be performed on such streams.
 */
@Slf4j
class StreamSegmentContainer extends AbstractService implements SegmentContainer {
    //region Members

    private final String traceObjectId;
    private final UpdateableContainerMetadata metadata;
    private final OperationLog durableLog;
    private final Cache readIndex;
    private final Storage storage;
    private final PendingAppendsCollection pendingAppendsCollection;
    private final StreamSegmentMapper segmentMapper;
    private final Executor executor;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentContainer class.
     *
     * @param streamSegmentContainerId The Id of the StreamSegmentContainer.
     * @param metadataRepository       The MetadataRepository to use.
     * @param durableLogFactory        The DurableLogFactory to use to create DurableLogs.
     * @param cacheFactory             The CacheFactory to use to create Read Indices.
     * @param storageFactory           The StorageFactory to use to create Storage Adapters.
     * @param executor                 An Executor that can be used to run async tasks.
     */
    public StreamSegmentContainer(String streamSegmentContainerId, MetadataRepository metadataRepository, OperationLogFactory durableLogFactory, CacheFactory cacheFactory, StorageFactory storageFactory, Executor executor) {
        Exceptions.checkNotNullOrEmpty(streamSegmentContainerId, "streamSegmentContainerId");
        Preconditions.checkNotNull(metadataRepository, "metadataRepository");
        Preconditions.checkNotNull(durableLogFactory, "durableLogFactory");
        Preconditions.checkNotNull(cacheFactory, "cacheFactory");
        Preconditions.checkNotNull(storageFactory, "storageFactory");
        Preconditions.checkNotNull(executor, "executor");

        this.traceObjectId = String.format("SegmentContainer[%s]", streamSegmentContainerId);
        this.storage = storageFactory.getStorageAdapter();
        this.metadata = metadataRepository.getMetadata(streamSegmentContainerId);
        this.readIndex = cacheFactory.createCache(this.metadata);
        this.executor = executor;
        this.durableLog = durableLogFactory.createDurableLog(metadata, readIndex);
        this.durableLog.addListener(new ServiceFailureListener(this::durableLogStoppedHandler, this::durableLogFailedHandler), this.executor);
        this.pendingAppendsCollection = new PendingAppendsCollection();
        this.segmentMapper = new StreamSegmentMapper(this.metadata, this.durableLog, this.storage);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            stopAsync().awaitTerminated();

            this.pendingAppendsCollection.close();
            this.durableLog.close();
            this.readIndex.close();
            this.closed = true;
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        int traceId = LoggerHelpers.traceEnter(log, traceObjectId, "doStart");
        this.durableLog.startAsync();
        this.executor.execute(() -> {
            this.durableLog.awaitRunning();
            LoggerHelpers.traceLeave(log, traceObjectId, "doStart", traceId);
            notifyStarted();
        });
    }

    @Override
    protected void doStop() {
        int traceId = LoggerHelpers.traceEnter(log, traceObjectId, "doStop");
        this.durableLog.stopAsync();
        this.executor.execute(() -> {
            this.durableLog.awaitTerminated();
            LoggerHelpers.traceLeave(log, traceObjectId, "doStop", traceId);
            this.notifyStopped();
        });
    }

    //endregion

    //region Container Implementation

    @Override
    public String getId() {
        return this.metadata.getContainerId();
    }

    //endregion

    //region StreamSegmentStore Implementation

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, byte[] data, AppendContext appendContext, Duration timeout) {
        ensureRunning();

        logRequest("append", streamSegmentName, data.length, appendContext);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                .thenCompose(streamSegmentId ->
                {
                    StreamSegmentAppendOperation operation = new StreamSegmentAppendOperation(streamSegmentId, data, appendContext);
                    CompletableFuture<Long> result = this.durableLog.add(operation, timer.getRemaining());

                    // Add to Append Context Registry, if needed.
                    this.pendingAppendsCollection.register(operation, result);
                    return result.thenApply(seqNo -> operation.getStreamSegmentOffset());
                });
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        ensureRunning();

        logRequest("read", streamSegmentName, offset, maxLength);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                .thenApply(streamSegmentId -> this.readIndex.read(streamSegmentId, offset, maxLength, timer.getRemaining()));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("getStreamSegmentInfo", streamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                .thenApply(streamSegmentId ->
                {
                    SegmentMetadata sm = this.metadata.getStreamSegmentMetadata(streamSegmentId);
                    return new StreamSegmentInformation(streamSegmentName, sm.getDurableLogLength(), sm.isSealed(), sm.isDeleted(), new Date());
                });
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("createStreamSegment", streamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.createNewStreamSegment(streamSegmentName, timer.getRemaining());
    }

    @Override
    public CompletableFuture<String> createBatch(String parentStreamName, Duration timeout) {
        ensureRunning();

        logRequest("createBatch", parentStreamName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.createNewBatchStreamSegment(parentStreamName, timer.getRemaining());
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("deleteStreamSegment", streamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // metadata.deleteStreamSegment will delete the given StreamSegment and all batches associated with it.
        // It returns a collection of names of StreamSegments that were deleted.
        Collection<String> streamSegmentsToDelete = this.metadata.deleteStreamSegment(streamSegmentName);
        CompletableFuture[] deletionFutures = new CompletableFuture[streamSegmentsToDelete.size()];
        int count = 0;
        for (String s : streamSegmentsToDelete) {
            deletionFutures[count] = this.storage.delete(s, timer.getRemaining());
            count++;
        }

        // Remove from Read Index.
        this.readIndex.performGarbageCollection();
        return CompletableFuture.allOf(deletionFutures);
    }

    @Override
    public CompletableFuture<Long> mergeBatch(String batchStreamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("mergeBatch", batchStreamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(batchStreamSegmentName, timer.getRemaining())
                .thenCompose(batchStreamSegmentId ->
                {
                    SegmentMetadata batchMetadata = this.metadata.getStreamSegmentMetadata(batchStreamSegmentId);
                    if (batchMetadata == null) {
                        throw new CompletionException(new StreamSegmentNotExistsException(batchStreamSegmentName));
                    }

                    Operation op = new MergeBatchOperation(batchMetadata.getParentId(), batchMetadata.getId());
                    return this.durableLog.add(op, timer.getRemaining());
                });
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("sealStreamSegment", streamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                .thenCompose(streamSegmentId ->
                {
                    Operation operation = new com.emc.logservice.server.logs.operations.StreamSegmentSealOperation(streamSegmentId);
                    return this.durableLog.add(operation, timer.getRemaining());
                });
    }

    @Override
    public CompletableFuture<AppendContext> getLastAppendContext(String streamSegmentName, UUID clientId) {
        ensureRunning();

        logRequest("getLastAppendContext", streamSegmentName, clientId);
        long streamSegmentId = this.metadata.getStreamSegmentId(streamSegmentName);
        if (streamSegmentId == StreamSegmentContainerMetadata.NoStreamSegmentId) {
            // We do not have any recent information about this StreamSegment. Do not bother to create an entry with it using SegmentMapper.
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<AppendContext> result = this.pendingAppendsCollection.get(streamSegmentId, clientId);
        if (result == null) {
            // No appends pending for this StreamSegment/ClientId combination; check metadata.
            SegmentMetadata segmentMetadata = this.metadata.getStreamSegmentMetadata(streamSegmentId);
            if (segmentMetadata != null) {
                result = CompletableFuture.completedFuture(segmentMetadata.getLastAppendContext(clientId));
            }
        }

        return result;
    }

    //endregion

    //region Helpers

    private void ensureRunning() {
        Exceptions.checkNotClosed(this.closed, this);
        if (state() != State.RUNNING) {
            throw new IllegalContainerStateException(this.getId(), state(), State.RUNNING);
        }
    }

    private void logRequest(String requestName, Object... args) {
        log.info("{}: {} {}", this.traceObjectId, requestName, args);
    }

    private void durableLogFailedHandler(Throwable cause) {
        // The Queue Processor failed. We need to shut down right away.
        log.warn("{}: DurableLog failed with exception {}", this.traceObjectId, cause);
        stopAsync().awaitTerminated();
        notifyFailed(cause);
    }

    private void durableLogStoppedHandler() {
        if (state() != State.STOPPING) {
            // The Queue Processor stopped but we are not in a stopping phase. We need to shut down right away.
            log.warn("{}: DurableLog stopped unexpectedly (no error) but StreamSegmentContainer was not currently stopping. Shutting down StreamSegmentContainer.", this.traceObjectId);
            stopAsync().awaitTerminated();
            notifyFailed(new StreamingException("DurableLog stopped unexpectedly (no error) but StreamSegmentContainer was not currently stopping."));
        }
    }

    //endregion
}
