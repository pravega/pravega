/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.service.server.containers;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Runnables;
import com.google.common.util.concurrent.Service;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.concurrent.ServiceShutdownListener;
import io.pravega.common.util.AsyncMap;
import io.pravega.service.contracts.AttributeUpdate;
import io.pravega.service.contracts.ReadResult;
import io.pravega.service.contracts.SegmentProperties;
import io.pravega.service.contracts.StreamSegmentNotExistsException;
import io.pravega.service.server.ContainerMetadata;
import io.pravega.service.server.IllegalContainerStateException;
import io.pravega.service.server.OperationLog;
import io.pravega.service.server.OperationLogFactory;
import io.pravega.service.server.ReadIndex;
import io.pravega.service.server.ReadIndexFactory;
import io.pravega.service.server.SegmentContainer;
import io.pravega.service.server.SegmentMetadata;
import io.pravega.service.server.Writer;
import io.pravega.service.server.WriterFactory;
import io.pravega.service.server.logs.operations.MergeTransactionOperation;
import io.pravega.service.server.logs.operations.Operation;
import io.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.service.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.service.server.logs.operations.UpdateAttributesOperation;
import io.pravega.service.storage.Storage;
import io.pravega.service.storage.StorageFactory;
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
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            stopAsync();
            ServiceShutdownListener.awaitShutdown(this, false);

            this.metadataCleaner.close();
            this.writer.close();
            this.durableLog.close();
            this.readIndex.close();
            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, traceObjectId, "doStart");
        log.info("{}: Starting.", this.traceObjectId);

        this.durableLog.startAsync();
        ExecutorServiceHelpers.execute(() -> {
            this.durableLog.awaitRunning();
            this.storage.initialize(this.metadata.getContainerEpoch());

            // DurableLog is running. Now start all other components that depend on it.
            this.metadataCleaner.startAsync();
            this.writer.startAsync();
            this.writer.awaitRunning();
            this.metadataCleaner.awaitRunning();
            log.info("{}: Started.", this.traceObjectId);
            LoggerHelpers.traceLeave(log, traceObjectId, "doStart", traceId);
            notifyStarted();
        }, this::doStop, Runnables.doNothing(), this.executor);
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
        this.metadataCleaner.stopAsync();
        this.writer.stopAsync();
        this.durableLog.stopAsync();
        ExecutorServiceHelpers.execute(() -> {
            ServiceShutdownListener.awaitShutdown(this.metadataCleaner, false);
            ServiceShutdownListener.awaitShutdown(this.writer, false);
            ServiceShutdownListener.awaitShutdown(this.durableLog, false);
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
        }, this::notifyFailed, Runnables.doNothing(), this.executor);
    }

    private Throwable getFailureCause(Service... services) {
        Throwable result = null;
        for (Service s : services) {
            if (s.state() == State.FAILED) {
                Throwable realEx = ExceptionHelpers.getRealException(s.failureCause());
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
        return FutureHelpers.toVoid(this.segmentMapper
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
        return FutureHelpers.toVoid(this.segmentMapper
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

        return segmentIdRetriever.thenApply(streamSegmentId -> this.metadata.getStreamSegmentMetadata(streamSegmentId).getSnapshot());
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

        val deletionFutures = new ArrayList<CompletableFuture<Void>>();
        for (SegmentMetadata toDelete : deletedSegments) {
            deletionFutures.add(this.storage
                    .openWrite(toDelete.getName())
                    .thenComposeAsync(handle -> this.storage.delete(handle, timer.getRemaining()), this.executor)
                    .thenComposeAsync(v -> this.stateStore.remove(toDelete.getName(), timer.getRemaining()), this.executor)
                    .exceptionally(ex -> {
                        ex = ExceptionHelpers.getRealException(ex);
                        if (ex instanceof StreamSegmentNotExistsException && toDelete.getParentId() != ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                            // We are ok if transactions are not found; they may have just been merged in and the metadata
                            // did not get a chance to get updated.
                            return null;
                        }

                        throw new CompletionException(ex);
                    }));
        }

        notifyMetadataRemoved(deletedSegments);
        return FutureHelpers.allOf(deletionFutures);
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
        component.addListener(new ServiceShutdownListener(stoppedHandler, failedHandler), this.executor);
    }

    //endregion
}