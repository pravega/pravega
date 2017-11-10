/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.common.util.AsyncMap;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.server.Writer;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.logs.operations.MergeTransactionOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
    private final StreamSegmentContainerMetadata metadata;
    private final OperationLog durableLog;
    private final ReadIndex readIndex;
    private final Writer writer;
    private final Storage storage;
    private final AsyncMap<String, SegmentState> stateStore;
    private final StreamSegmentMapper segmentMapper;
    private final ScheduledExecutorService executor;
    private final MetadataCleaner metadataCleaner;
    private final AtomicBoolean closed;
    private final SegmentStoreMetrics.Container metrics;

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
        this.storage = storageFactory.createStorageAdapter();
        this.metadata = new StreamSegmentContainerMetadata(streamSegmentContainerId, config.getMaxActiveSegmentCount());
        this.readIndex = readIndexFactory.createReadIndex(this.metadata, this.storage);
        this.executor = executor;
        this.durableLog = durableLogFactory.createDurableLog(this.metadata, this.readIndex);
        shutdownWhenStopped(this.durableLog, "DurableLog");
        this.writer = writerFactory.createWriter(this.metadata, this.durableLog, this.readIndex, this.storage);
        shutdownWhenStopped(this.writer, "Writer");
        this.stateStore = new SegmentStateStore(this.storage, this.executor);
        this.metadataCleaner = new MetadataCleaner(this.config, this.metadata, this.stateStore, this::notifyMetadataRemoved,
                this.executor, this.traceObjectId);
        shutdownWhenStopped(this.metadataCleaner, "MetadataCleaner");
        this.segmentMapper = new StreamSegmentMapper(this.metadata, this.durableLog, this.stateStore, this.metadataCleaner::runOnce,
                this.storage, this.executor);
        this.metrics = new SegmentStoreMetrics.Container(streamSegmentContainerId);
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            Futures.await(Services.stopAsync(this, this.executor));
            this.metadataCleaner.close();
            this.writer.close();
            this.durableLog.close();
            this.readIndex.close();
            this.storage.close();
            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, traceObjectId, "doStart");
        log.info("{}: Starting.", this.traceObjectId);

        Services.startAsync(this.durableLog, this.executor)
                .thenRunAsync(() -> this.storage.initialize(this.metadata.getContainerEpoch()), this.executor)
                .thenCompose(v -> CompletableFuture.allOf(
                        Services.startAsync(this.metadataCleaner, this.executor),
                        Services.startAsync(this.writer, this.executor)))
                .thenRun(() -> {
                    log.info("{}: Started.", this.traceObjectId);
                    LoggerHelpers.traceLeave(log, traceObjectId, "doStart", traceId);
                    notifyStarted();
                })
                .exceptionally(ex -> {
                    doStop(ex);
                    return null;
                });
    }

    @Override
    protected void doStop() {
        doStop(null);
    }

    /**
     * Stops the StreamSegmentContainer by stopping all components, waiting for them to stop, and reports a normal
     * shutdown or failure based on case. It will report a normal shutdown only if all components shut down normally
     * and cause is null. Otherwise, the container will report either the exception of the failed component, or the
     * given cause.
     *
     * @param cause (Optional) The failure cause. If any of the components failed as well, this will be added as a
     *              suppressed exception to the Service's failure cause.
     */
    private void doStop(Throwable cause) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, traceObjectId, "doStop");
        log.info("{}: Stopping.", this.traceObjectId);
        CompletableFuture.allOf(
                Services.stopAsync(this.metadataCleaner, this.executor),
                Services.stopAsync(this.writer, this.executor),
                Services.stopAsync(this.durableLog, this.executor))
                .whenCompleteAsync((r, ex) -> {
                    Throwable failureCause = getFailureCause(this.durableLog, this.writer, this.metadataCleaner);
                    if (failureCause == null) {
                        failureCause = cause;
                    } else if (cause != null && failureCause != cause) {
                        failureCause.addSuppressed(cause);
                    }

                    if (failureCause == null) {
                        // Normal shutdown
                        log.info("{}: Stopped.", this.traceObjectId);
                        LoggerHelpers.traceLeave(log, traceObjectId, "doStop", traceId);
                        notifyStopped();
                    } else {
                        // Shutting down due to failure.
                        log.warn("{}: Failed due to component failure.", this.traceObjectId);
                        LoggerHelpers.traceLeave(log, traceObjectId, "doStop", traceId);
                        notifyFailed(failureCause);
                    }
                }, this.executor)
                .exceptionally(ex -> {
                    notifyFailed(ex);
                    return null;
                });
    }

    private Throwable getFailureCause(Service... services) {
        Throwable result = null;
        for (Service s : services) {
            if (s.state() == State.FAILED) {
                Throwable realEx = Exceptions.unwrap(s.failureCause());
                if (result == null) {
                    result = realEx;
                } else {
                    result.addSuppressed(realEx);
                }
            }
        }

        return result;
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
        this.metrics.append();
        return this.segmentMapper.getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining(),
                streamSegmentId -> {
                    StreamSegmentAppendOperation operation = new StreamSegmentAppendOperation(streamSegmentId, data, attributeUpdates);
                    return this.durableLog.add(operation, timer.getRemaining());
                });
    }

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, long offset, byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        ensureRunning();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        logRequest("appendWithOffset", streamSegmentName, data.length);
        this.metrics.appendWithOffset();
        return this.segmentMapper.getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining(),
                streamSegmentId -> {
                    StreamSegmentAppendOperation operation = new StreamSegmentAppendOperation(streamSegmentId, offset, data, attributeUpdates);
                    return this.durableLog.add(operation, timer.getRemaining());
                });
    }

    @Override
    public CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        ensureRunning();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        logRequest("updateAttributes", streamSegmentName, attributeUpdates);
        this.metrics.updateAttributes();
        return this.segmentMapper.getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining(),
                streamSegmentId -> {
                    UpdateAttributesOperation operation = new UpdateAttributesOperation(streamSegmentId, attributeUpdates);
                    return this.durableLog.add(operation, timer.getRemaining());
                });
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        ensureRunning();

        logRequest("read", streamSegmentName, offset, maxLength);
        this.metrics.read();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining(),
                        streamSegmentId -> CompletableFuture.completedFuture(this.readIndex.read(streamSegmentId, offset, maxLength, timer.getRemaining())));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, boolean waitForPendingOps, Duration timeout) {
        ensureRunning();

        logRequest("getStreamSegmentInfo", streamSegmentName);
        this.metrics.getInfo();

        if (waitForPendingOps) {
            // We have been instructed to wait for all pending operations to complete. Use an op barrier and wait for it
            // before proceeding.
            TimeoutTimer timer = new TimeoutTimer(timeout);
            return this.durableLog
                    .operationProcessingBarrier(timer.getRemaining())
                    .thenComposeAsync(v -> this.segmentMapper.getStreamSegmentInfo(streamSegmentName, timer.getRemaining()), this.executor);
        } else {
            return this.segmentMapper.getStreamSegmentInfo(streamSegmentName, timeout);
        }
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Collection<AttributeUpdate> attributes, Duration timeout) {
        ensureRunning();

        logRequest("createStreamSegment", streamSegmentName);
        this.metrics.createSegment();
        return this.segmentMapper.createNewStreamSegment(streamSegmentName, attributes, timeout);
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentSegmentName, UUID transactionId, Collection<AttributeUpdate> attributes, Duration timeout) {
        ensureRunning();

        logRequest("createTransaction", parentSegmentName);
        this.metrics.createTxn();
        return this.segmentMapper.createNewTransactionStreamSegment(parentSegmentName, transactionId, attributes, timeout);
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("deleteStreamSegment", streamSegmentName);
        this.metrics.deleteSegment();
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // metadata.deleteStreamSegment will delete the given StreamSegment and all Transactions associated with it.
        // It returns a mapping of segment ids to names of StreamSegments that were deleted.
        // As soon as this happens, all operations that deal with those segments will start throwing appropriate exceptions
        // or ignore the segments altogether (such as StorageWriter).
        Collection<SegmentMetadata> deletedSegments = this.metadata.deleteStreamSegment(streamSegmentName);

        val deletionFutures = new ArrayList<CompletableFuture<Void>>();
        for (SegmentMetadata toDelete : deletedSegments) {
            deletionFutures.add(this.storage
                    .openWrite(toDelete.getName())
                    .thenComposeAsync(handle -> this.storage.delete(handle, timer.getRemaining()), this.executor)
                    .thenComposeAsync(v -> this.stateStore.remove(toDelete.getName(), timer.getRemaining()), this.executor)
                    .exceptionally(ex -> {
                        ex = Exceptions.unwrap(ex);
                        if (ex instanceof StreamSegmentNotExistsException && toDelete.isTransaction()) {
                            // We are ok if transactions are not found; they may have just been merged in and the metadata
                            // did not get a chance to get updated.
                            return null;
                        }

                        throw new CompletionException(ex);
                    }));
        }

        notifyMetadataRemoved(deletedSegments);
        return Futures.allOf(deletionFutures);
    }

    @Override
    public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
        ensureRunning();

        logRequest("truncateStreamSegment", streamSegmentName);
        this.metrics.truncate();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining(),
                        streamSegmentId -> {
                            StreamSegmentTruncateOperation op = new StreamSegmentTruncateOperation(streamSegmentId, offset);
                            return this.durableLog.add(op, timer.getRemaining());
                        });
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        ensureRunning();

        logRequest("mergeTransaction", transactionName);
        this.metrics.mergeTxn();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper
                .getOrAssignStreamSegmentId(transactionName, timer.getRemaining(),
                        transactionId -> {
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
        this.metrics.seal();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        AtomicReference<StreamSegmentSealOperation> operation = new AtomicReference<>();
        return this.segmentMapper
                .getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining(),
                        streamSegmentId -> {
                    operation.set(new StreamSegmentSealOperation(streamSegmentId));
                    return this.durableLog.add(operation.get(), timer.getRemaining());
                })
                .thenApply(seqNo -> operation.get().getStreamSegmentOffset());
    }

    //endregion

    //region SegmentContainer Implementation

    @Override
    public Collection<SegmentProperties> getActiveSegments() {
        ensureRunning();
        logRequest("getActiveSegments");

        // To reduce locking in the metadata, we first get the list of Segment Ids, then we fetch their metadata
        // one by one. This only locks the metadata on the first call and, individually, on each call to getStreamSegmentMetadata.
        return new ArrayList<>(this.metadata.getAllStreamSegmentIds())
                .stream()
                .map(this.metadata::getStreamSegmentMetadata)
                .filter(Objects::nonNull)
                .map(SegmentMetadata::getSnapshot)
                .collect(Collectors.toList());
    }

    //endregion

    //region Helpers

    /**
     * Callback that notifies eligible components that the given Segments' metadatas has been removed from the metadata,
     * regardless of the trigger (eviction or deletion).
     *
     * @param segments A Collection of SegmentMetadatas for those segments which were removed.
     */
    protected void notifyMetadataRemoved(Collection<SegmentMetadata> segments) {
        if (segments.size() > 0) {
            this.readIndex.cleanup(segments.stream().map(SegmentMetadata::getId).iterator());
        }
    }

    private void ensureRunning() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        if (state() != State.RUNNING) {
            throw new IllegalContainerStateException(this.getId(), state(), State.RUNNING);
        }
    }

    private void logRequest(String requestName, Object... args) {
        log.debug("{}: {} {}", this.traceObjectId, requestName, args);
    }

    private void shutdownWhenStopped(Service component, String componentName) {
        Consumer<Throwable> failedHandler = cause -> {
            log.warn("{}: {} failed. Shutting down StreamSegmentContainer.", this.traceObjectId, componentName, cause);
            if (state() == State.RUNNING) {
                // We can only stop the service if it's already running. During the stop it will pick up the failure cause
                // and terminate in failure.
                stopAsync();
            } else if (state() == State.STARTING) {
                // We can only notify failed if we are starting. We cannot fail a service if it's already in a terminal state.
                notifyFailed(cause);
            }
        };
        Runnable stoppedHandler = () -> {
            if (state() == State.STARTING || state() == State.RUNNING) {
                // The Component stopped but we are not in a stopping/terminal phase. We need to shut down right away.
                log.warn("{}: {} stopped unexpectedly (no error) but StreamSegmentContainer was not currently stopping. Shutting down StreamSegmentContainer.",
                        this.traceObjectId,
                        componentName);
                stopAsync();
            }
        };
        Services.onStop(component, stoppedHandler, failedHandler, this.executor);
    }

    //endregion
}