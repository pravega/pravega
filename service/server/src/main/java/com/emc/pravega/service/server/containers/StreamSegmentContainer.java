/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.concurrent.ServiceShutdownListener;
import com.emc.pravega.common.util.AsyncMap;
import com.emc.pravega.common.util.ImmutableDate;
import com.emc.pravega.service.contracts.AttributeUpdate;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentInformation;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.server.IllegalContainerStateException;
import com.emc.pravega.service.server.OperationLog;
import com.emc.pravega.service.server.OperationLogFactory;
import com.emc.pravega.service.server.ReadIndex;
import com.emc.pravega.service.server.ReadIndexFactory;
import com.emc.pravega.service.server.SegmentContainer;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.Writer;
import com.emc.pravega.service.server.WriterFactory;
import com.emc.pravega.service.server.logs.operations.MergeTransactionOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentSealOperation;
import com.emc.pravega.service.server.logs.operations.UpdateAttributesOperation;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageFactory;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Container for StreamSegments. All StreamSegments that are related (based on a hashing functions) will belong to the
 * same StreamSegmentContainer. Handles all operations that can be performed on such streams.
 */
@Slf4j
class StreamSegmentContainer extends AbstractService implements SegmentContainer {
    //region Members
    private final String traceObjectId;
    private final ContainerConfig config;
    private final UpdateableContainerMetadata metadata;
    private final OperationLog durableLog;
    private final ReadIndex readIndex;
    private final Writer writer;
    private final Storage storage;
    private final AsyncMap<String, SegmentState> stateStore;
    private final StreamSegmentMapper segmentMapper;
    private final ScheduledExecutorService executor;
    private CompletableFuture<Void> cleanupTask;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentContainer class.
     *
     * @param streamSegmentContainerId The Id of the StreamSegmentContainer.
     * @param config                   The ContainerConfig to use for this StreamSegmentContainer.
     * @param durableLogFactory        The DurableLogFactory to use to create DurableLogs.
     * @param readIndexFactory         The ReadIndexFactory to use to create Read Indices.
     * @param writerFactory            The WriterFactory to use to create Writers.
     * @param storageFactory           The StorageFactory to use to create Storage Adapters.
     * @param executor                 An Executor that can be used to run async tasks.
     */
    StreamSegmentContainer(int streamSegmentContainerId, ContainerConfig config, OperationLogFactory durableLogFactory, ReadIndexFactory readIndexFactory,
                           WriterFactory writerFactory, StorageFactory storageFactory, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(durableLogFactory, "durableLogFactory");
        Preconditions.checkNotNull(readIndexFactory, "readIndexFactory");
        Preconditions.checkNotNull(writerFactory, "writerFactory");
        Preconditions.checkNotNull(storageFactory, "storageFactory");
        Preconditions.checkNotNull(executor, "executor");

        this.traceObjectId = String.format("SegmentContainer[%d]", streamSegmentContainerId);
        this.config = config;
        this.storage = storageFactory.getStorageAdapter();
        this.metadata = new StreamSegmentContainerMetadata(streamSegmentContainerId);
        this.readIndex = readIndexFactory.createReadIndex(this.metadata);
        this.executor = executor;
        this.durableLog = durableLogFactory.createDurableLog(this.metadata, this.readIndex);
        this.durableLog.addListener(new ServiceShutdownListener(createComponentStoppedHandler("DurableLog"), createComponentFailedHandler("DurableLog")), this.executor);
        this.writer = writerFactory.createWriter(this.metadata, this.durableLog, this.readIndex);
        this.writer.addListener(new ServiceShutdownListener(createComponentStoppedHandler("Writer"), createComponentFailedHandler("Writer")), this.executor);
        this.stateStore = new SegmentStateStore(this.storage, this.executor);
        this.segmentMapper = new StreamSegmentMapper(this.metadata, this.durableLog, this.stateStore, this.storage, this.executor);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            stopAsync().awaitTerminated();

            stopCleanupTask();
            this.writer.close();
            this.durableLog.close();
            this.readIndex.close();
            log.info("{}: Closed.", this.traceObjectId);
            this.closed = true;
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        long traceId = LoggerHelpers.traceEnter(log, traceObjectId, "doStart");
        log.info("{}: Starting.", this.traceObjectId);
        this.cleanupTask = FutureHelpers.loop(
                () -> state() == State.STARTING || isRunning(),
                () -> FutureHelpers.delayedFuture(this.config.getSegmentMetadataExpiration(), this.executor)
                                   .thenCompose(this::metadataCleanup),
                null,
                this.executor);

        this.durableLog.startAsync();
        this.executor.execute(() -> {
            this.durableLog.awaitRunning();

            // DurableLog is running. Now start Writer.
            this.writer.startAsync();
            this.executor.execute(() -> {
                this.writer.awaitRunning();
                log.info("{}: Started.", this.traceObjectId);
                LoggerHelpers.traceLeave(log, traceObjectId, "doStart", traceId);
                notifyStarted();
            });
        });
    }

    @Override
    protected void doStop() {
        long traceId = LoggerHelpers.traceEnter(log, traceObjectId, "doStop");
        log.info("{}: Stopping.", this.traceObjectId);
        this.writer.stopAsync();
        this.durableLog.stopAsync();
        this.executor.execute(() -> {
            this.writer.awaitTerminated();
            this.durableLog.awaitTerminated();
            stopCleanupTask();
            log.info("{}: Stopped.", this.traceObjectId);
            LoggerHelpers.traceLeave(log, traceObjectId, "doStop", traceId);
            this.notifyStopped();
        });
    }

    //endregion

    //region Container Implementation

    @Override
    public int getId() {
        return this.metadata.getContainerId();
    }

    //endregion

    //region StreamSegmentStore Implementation

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        ensureRunning();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        logRequest("append", streamSegmentName, data.length);
        return FutureHelpers.toVoid(segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                .thenCompose(streamSegmentId -> {
                    StreamSegmentAppendOperation operation = new StreamSegmentAppendOperation(streamSegmentId, data, attributeUpdates);
                    return this.durableLog.add(operation, timer.getRemaining());
                }));
    }

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, long offset, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        ensureRunning();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        logRequest("appendWithOffset", streamSegmentName, data.length);
        return FutureHelpers.toVoid(this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                .thenCompose(streamSegmentId -> {
                    StreamSegmentAppendOperation operation = new StreamSegmentAppendOperation(streamSegmentId, offset, data, attributeUpdates);
                    return this.durableLog.add(operation, timer.getRemaining());
                }));
    }

    @Override
    public CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        ensureRunning();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        logRequest("updateAttributes", streamSegmentName, attributeUpdates);
        return FutureHelpers.toVoid(segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                .thenCompose(streamSegmentId -> {
                    UpdateAttributesOperation operation = new UpdateAttributesOperation(streamSegmentId, attributeUpdates);
                    return this.durableLog.add(operation, timer.getRemaining());
                }));
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
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, boolean waitForPendingOps, Duration timeout) {
        ensureRunning();

        logRequest("getStreamSegmentInfo", streamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);

        CompletableFuture<Long> segmentIdRetriever;
        if (waitForPendingOps) {
            // We have been instructed to wait for all pending operations to complete. Use an op barrier and wait for it
            // before proceeding.
            segmentIdRetriever = this.durableLog
                    .operationProcessingBarrier(timer.getRemaining())
                    .thenComposeAsync(v -> this.segmentMapper.getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining()), this.executor);
        } else {
            segmentIdRetriever = this.segmentMapper.getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining());
        }

        return segmentIdRetriever
                .thenApply(streamSegmentId -> {
                    SegmentMetadata sm = this.metadata.getStreamSegmentMetadata(streamSegmentId);
                    return new StreamSegmentInformation(streamSegmentName,
                            sm.getDurableLogLength(),
                            sm.isSealed(),
                            sm.isDeleted(),
                            new HashMap<>(sm.getAttributes()),
                            new ImmutableDate());
                });
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        ensureRunning();

        logRequest("createStreamSegment", streamSegmentName);
        return this.segmentMapper.createNewStreamSegment(streamSegmentName, attributes, timeout);
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentSegmentName, UUID transactionId, Collection<AttributeUpdate> attributes, Duration timeout) {
        ensureRunning();

        logRequest("createTransaction", parentSegmentName);
        return this.segmentMapper.createNewTransactionStreamSegment(parentSegmentName, transactionId, attributes, timeout);
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("deleteStreamSegment", streamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // metadata.deleteStreamSegment will delete the given StreamSegment and all Transactions associated with it.
        // It returns a mapping of segment ids to names of StreamSegments that were deleted.
        // As soon as this happens, all operations that deal with those segments will start throwing appropriate exceptions
        // or ignore the segments altogether (such as StorageWriter).
        Collection<SegmentMetadata> deletedSegments = this.metadata.deleteStreamSegment(streamSegmentName);
        CompletableFuture[] deletionFutures = mapToFutureArray(
                deletedSegments,
                s -> this.storage
                        .delete(s.getName(), timer.getRemaining())
                        .thenComposeAsync(v -> this.stateStore.remove(s.getName(), timer.getRemaining()), this.executor));

        removeFromReadIndex(deletedSegments);
        return CompletableFuture.allOf(deletionFutures);
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        ensureRunning();

        logRequest("mergeTransaction", transactionName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(transactionName, timer.getRemaining())
                .thenCompose(transactionId -> {
                    SegmentMetadata transactionMetadata = this.metadata.getStreamSegmentMetadata(transactionId);
                    if (transactionMetadata == null) {
                        throw new CompletionException(new StreamSegmentNotExistsException(transactionName));
                    }

                    Operation op = new MergeTransactionOperation(transactionMetadata.getParentId(), transactionMetadata.getId());
                    return this.durableLog.add(op, timer.getRemaining());
                })
                .thenComposeAsync(v -> this.stateStore.remove(transactionName, timer.getRemaining()), this.executor);
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("sealStreamSegment", streamSegmentName);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        AtomicReference<StreamSegmentSealOperation> operation = new AtomicReference<>();
        return this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                .thenCompose(streamSegmentId -> {
                    operation.set(new StreamSegmentSealOperation(streamSegmentId));
                    return this.durableLog.add(operation.get(), timer.getRemaining());
                })
                .thenApply(seqNo -> operation.get().getStreamSegmentOffset());
    }

    //endregion

    //region Helpers

    protected CompletableFuture<Boolean> metadataCleanup(Void ignored) {
        if (this.closed || !isRunning()) {
            stopCleanupTask();
            return CompletableFuture.completedFuture(false);
        }

        long traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "metadataCleanup");

        // Evict segments from the metadata.
        Duration expiration = this.config.getSegmentMetadataExpiration();
        Collection<SegmentMetadata> cleanupCandidates = this.metadata.getEvictionCandidates(expiration);

        // Serialize only those segments that are still alive (not deleted or merged - those will get removed anyway).
        val cleanupTasks = cleanupCandidates
                .stream()
                .filter(sm -> !sm.isDeleted() || !sm.isMerged())
                .map(sm -> this.stateStore.put(sm.getName(), new SegmentState(sm), this.config.getSegmentMetadataExpiration()))
                .collect(Collectors.toList());

        return FutureHelpers
                .allOf(cleanupTasks)
                .thenApply(v -> {
                    Collection<SegmentMetadata> evictedSegments = this.metadata.cleanup(cleanupCandidates, expiration);
                    removeFromReadIndex(evictedSegments);
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "metadataCleanup", traceId, evictedSegments.size());
                    return evictedSegments.size() > 0;
                });
    }

    private void removeFromReadIndex(Collection<SegmentMetadata> segments) {
        if (segments.size() > 0) {
            this.readIndex.cleanup(segments.stream().map(SegmentMetadata::getId).iterator());
        }
    }

    private void stopCleanupTask() {
        CompletableFuture<?> cleanupTask = this.cleanupTask;
        if (cleanupTask != null && !cleanupTask.isDone()) {
            cleanupTask.cancel(true);
            this.cleanupTask = null;
        }
    }

    private void ensureRunning() {
        Exceptions.checkNotClosed(this.closed, this);
        if (state() != State.RUNNING) {
            throw new IllegalContainerStateException(this.getId(), state(), State.RUNNING);
        }
    }

    private void logRequest(String requestName, Object... args) {
        log.info("{}: {} {}", this.traceObjectId, requestName, args);
    }

    private Consumer<Throwable> createComponentFailedHandler(String componentName) {
        return cause -> {
            log.warn("{}: {} failed with exception {}.", this.traceObjectId, componentName, cause);
            if (state() != State.STARTING) {
                // We cannot stop the service while we're starting it.
                stopAsync().awaitTerminated();
            }

            notifyFailed(cause);
        };
    }

    private Runnable createComponentStoppedHandler(String componentName) {
        return () -> {
            if (state() != State.STOPPING) {
                // The Queue Processor stopped but we are not in a stopping phase. We need to shut down right away.
                log.warn("{}: {} stopped unexpectedly (no error) but StreamSegmentContainer was not currently stopping. Shutting down StreamSegmentContainer.",
                        this.traceObjectId,
                        componentName);
                stopAsync().awaitTerminated();
            }
        };
    }

    private <T> CompletableFuture[] mapToFutureArray(Collection<T> source, Function<T, CompletableFuture> transformer) {
        CompletableFuture[] result = new CompletableFuture[source.size()];
        int count = 0;
        for (T s : source) {
            result[count++] = transformer.apply(s);
        }

        return result;
    }

    //endregion
}