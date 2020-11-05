/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.Retry;
import io.pravega.common.util.Retry.RetryAndThrowConditionally;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.MergeStreamSegmentResult;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.SegmentType;
import io.pravega.segmentstore.contracts.StreamSegmentMergedException;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.AttributeIterator;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.ContainerOfflineException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.ReadIndexFactory;
import io.pravega.segmentstore.server.SegmentContainer;
import io.pravega.segmentstore.server.SegmentContainerExtension;
import io.pravega.segmentstore.server.SegmentContainerFactory;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.Writer;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.attributes.AttributeIndexFactory;
import io.pravega.segmentstore.server.attributes.ContainerAttributeIndex;
import io.pravega.segmentstore.server.logs.PriorityCalculator;
import io.pravega.segmentstore.server.logs.operations.AttributeUpdaterOperation;
import io.pravega.segmentstore.server.logs.operations.DeleteSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.segmentstore.server.logs.operations.OperationType;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentMapOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.server.logs.operations.UpdateAttributesOperation;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.metadata.TableBasedMetadataStore;
import io.pravega.shared.NameUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Container for StreamSegments. All StreamSegments that are related (based on a hashing functions) will belong to the
 * same StreamSegmentContainer. Handles all operations that can be performed on such streams.
 */
@Slf4j
class StreamSegmentContainer extends AbstractService implements SegmentContainer {
    //region Members
    private static final RetryAndThrowConditionally CACHE_ATTRIBUTES_RETRY = Retry.withExpBackoff(50, 2, 10, 1000)
            .retryWhen(ex -> ex instanceof BadAttributeUpdateException);
    protected final StreamSegmentContainerMetadata metadata;
    private final String traceObjectId;
    private final OperationLog durableLog;
    private final ReadIndex readIndex;
    private final ContainerAttributeIndex attributeIndex;
    private final Writer writer;
    private final Storage storage;
    private final MetadataStore metadataStore;
    private final ScheduledExecutorService executor;
    private final MetadataCleaner metadataCleaner;
    private final AtomicBoolean closed;
    private final SegmentStoreMetrics.Container metrics;
    private final Map<Class<? extends SegmentContainerExtension>, ? extends SegmentContainerExtension> extensions;
    private final ContainerConfig config;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentContainer class.
     *
     * @param streamSegmentContainerId The Id of the StreamSegmentContainer.
     * @param config                   The ContainerConfig to use for this StreamSegmentContainer.
     * @param durableLogFactory        The DurableLogFactory to use to create DurableLogs.
     * @param readIndexFactory         The ReadIndexFactory to use to create Read Indices.
     * @param attributeIndexFactory    The AttributeIndexFactory to use to create Attribute Indices.
     * @param writerFactory            The WriterFactory to use to create Writers.
     * @param storageFactory           The StorageFactory to use to create Storage Adapters.
     * @param createExtensions            A Function that, given an instance of this class, will create the set of
     *                                 {@link SegmentContainerExtension}s to be associated with that instance.
     * @param executor                 An Executor that can be used to run async tasks.
     */
    StreamSegmentContainer(int streamSegmentContainerId, ContainerConfig config, OperationLogFactory durableLogFactory, ReadIndexFactory readIndexFactory,
                           AttributeIndexFactory attributeIndexFactory, WriterFactory writerFactory, StorageFactory storageFactory,
                           SegmentContainerFactory.CreateExtensions createExtensions, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(durableLogFactory, "durableLogFactory");
        Preconditions.checkNotNull(readIndexFactory, "readIndexFactory");
        Preconditions.checkNotNull(writerFactory, "writerFactory");
        Preconditions.checkNotNull(storageFactory, "storageFactory");
        Preconditions.checkNotNull(executor, "executor");

        this.traceObjectId = String.format("SegmentContainer[%d]", streamSegmentContainerId);
        this.storage = storageFactory.createStorageAdapter();
        this.metadata = new StreamSegmentContainerMetadata(streamSegmentContainerId, config.getMaxActiveSegmentCount());
        this.readIndex = readIndexFactory.createReadIndex(this.metadata, this.storage);
        this.executor = executor;
        this.config = config;
        this.durableLog = durableLogFactory.createDurableLog(this.metadata, this.readIndex);
        shutdownWhenStopped(this.durableLog, "DurableLog");
        this.attributeIndex = attributeIndexFactory.createContainerAttributeIndex(this.metadata, this.storage);
        this.writer = writerFactory.createWriter(this.metadata, this.durableLog, this.readIndex, this.attributeIndex, this.storage, this::createWriterProcessors);
        shutdownWhenStopped(this.writer, "Writer");
        this.extensions = Collections.unmodifiableMap(createExtensions.apply(this, this.executor));
        this.metadataStore = createMetadataStore();
        this.metadataCleaner = new MetadataCleaner(config, this.metadata, this.metadataStore, this::notifyMetadataRemoved,
                this.executor, this.traceObjectId);
        shutdownWhenStopped(this.metadataCleaner, "MetadataCleaner");
        this.metrics = new SegmentStoreMetrics.Container(streamSegmentContainerId);
        this.closed = new AtomicBoolean();
    }

    private MetadataStore createMetadataStore() {
        MetadataStore.Connector connector = new MetadataStore.Connector(this.metadata, this::mapSegmentId,
                this::deleteSegmentImmediate, this::deleteSegmentDelayed, this::runMetadataCleanup);
        ContainerTableExtension tableExtension = getExtension(ContainerTableExtension.class);
        Preconditions.checkArgument(tableExtension != null, "ContainerTableExtension required for initialization.");
        return new TableMetadataStore(connector, tableExtension, this.executor);
    }

    /**
     * Creates WriterSegmentProcessors for the given Segment Metadata from all registered Extensions.
     *
     * @param segmentMetadata The Segment Metadata to create WriterSegmentProcessors for.
     * @return A Collection of processors.
     */
    private Collection<WriterSegmentProcessor> createWriterProcessors(UpdateableSegmentMetadata segmentMetadata) {
        ImmutableList.Builder<WriterSegmentProcessor> builder = ImmutableList.builder();
        this.extensions.values().forEach(p -> builder.addAll(p.createWriterSegmentProcessors(segmentMetadata)));
        return builder.build();
    }

    /**
     * Initializes storage.
     *
     * @throws Exception
     */
    private void initializeStorage() throws Exception {
        this.storage.initialize(this.metadata.getContainerEpoch());

        if (this.storage instanceof ChunkedSegmentStorage) {
            ChunkedSegmentStorage chunkedStorage = (ChunkedSegmentStorage) this.storage;

            // Initialize storage metadata table segment
            ContainerTableExtension tableExtension = getExtension(ContainerTableExtension.class);
            String s = NameUtils.getStorageMetadataSegmentName(this.metadata.getContainerId());

            val metadata = new TableBasedMetadataStore(s, tableExtension);

            // Bootstrap
            chunkedStorage.bootstrap(this.metadata.getContainerId(), metadata);
        }
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            this.metadataStore.close();
            this.extensions.values().forEach(SegmentContainerExtension::close);
            Futures.await(Services.stopAsync(this, this.executor));
            this.metadataCleaner.close();
            this.writer.close();
            this.durableLog.close();
            this.readIndex.close();
            this.storage.close();
            this.metrics.close();
            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        log.info("{}: Starting.", this.traceObjectId);

        Services.startAsync(this.durableLog, this.executor)
                .thenComposeAsync(v -> startWhenDurableLogOnline(), this.executor)
                .whenComplete((v, ex) -> {
                    if (ex == null) {
                        // We are started and ready to accept requests when DurableLog starts. All other (secondary) services
                        // are not required for accepting new operations and can still start in the background.
                        notifyStarted();
                    } else {
                        doStop(ex);
                    }
                });
    }

    private CompletableFuture<Void> startWhenDurableLogOnline() {
        CompletableFuture<Void> isReady;
        CompletableFuture<Void> delayedStart;
        if (this.durableLog.isOffline()) {
            // Attach a listener to the DurableLog's awaitOnline() Future and initiate the services' startup when that
            // completes successfully.
            log.info("{}: DurableLog is OFFLINE. Not starting secondary services yet.", this.traceObjectId);
            isReady = CompletableFuture.completedFuture(null);
            delayedStart = this.durableLog.awaitOnline()
                    .thenComposeAsync(v -> initializeSecondaryServices(), this.executor);
        } else {
            // DurableLog is already online. Immediately initialize secondary services. In this particular case, it needs
            // to be done synchronously since we need to initialize Storage before notifying that we are fully started.
            isReady = initializeSecondaryServices();
            delayedStart = isReady;
        }

        // Delayed start. Secondary services need not be started in order for us to accept requests.
        delayedStart.thenComposeAsync(v -> startSecondaryServicesAsync(), this.executor)
                .whenComplete((v, ex) -> {
                    if (ex == null) {
                        // Successful start.
                        log.info("{}: Started.", this.traceObjectId);
                    } else if (!(Exceptions.unwrap(ex) instanceof ObjectClosedException) || !Services.isTerminating(state())) {
                        // If the delayed start fails, immediately shut down the Segment Container with the appropriate
                        // exception. We should ignore ObjectClosedExceptions or other exceptions during a shutdown phase
                        // since that's most likely due to us shutting down.
                        doStop(ex);
                    }
                });

        return isReady;
    }

    private CompletableFuture<Void> initializeSecondaryServices() {
        try {
            initializeStorage();
        } catch (Exception ex) {
            return Futures.failedFuture(ex);
        }
        return this.metadataStore.initialize(this.config.getMetadataStoreInitTimeout());
    }

    private CompletableFuture<Void> startSecondaryServicesAsync() {
        return CompletableFuture.allOf(
                Services.startAsync(this.metadataCleaner, this.executor),
                Services.startAsync(this.writer, this.executor));
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

    @Override
    public boolean isOffline() {
        return this.durableLog.isOffline();
    }

    //endregion

    //region StreamSegmentStore Implementation

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, BufferView data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        ensureRunning();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        logRequest("append", streamSegmentName, data.getLength());
        this.metrics.append();
        return this.metadataStore.getOrAssignSegmentId(streamSegmentName, timer.getRemaining(),
                streamSegmentId -> {
                    val operation = new StreamSegmentAppendOperation(streamSegmentId, data, attributeUpdates);
                    return processAppend(operation, timer).thenApply(v -> operation.getLastStreamSegmentOffset());
                });
    }

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, long offset, BufferView data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        ensureRunning();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        logRequest("appendWithOffset", streamSegmentName, data.getLength());
        this.metrics.appendWithOffset();
        return this.metadataStore.getOrAssignSegmentId(streamSegmentName, timer.getRemaining(),
                streamSegmentId -> {
                    val operation = new StreamSegmentAppendOperation(streamSegmentId, offset, data, attributeUpdates);
                    return processAppend(operation, timer).thenApply(v -> operation.getLastStreamSegmentOffset());
                });
    }

    @Override
    public CompletableFuture<Void> updateAttributes(String streamSegmentName, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        ensureRunning();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        logRequest("updateAttributes", streamSegmentName, attributeUpdates);
        this.metrics.updateAttributes();
        return this.metadataStore.getOrAssignSegmentId(streamSegmentName, timer.getRemaining(),
                streamSegmentId -> updateAttributesForSegment(streamSegmentId, attributeUpdates, timer.getRemaining()));
    }

    @Override
    public CompletableFuture<Map<UUID, Long>> getAttributes(String streamSegmentName, Collection<UUID> attributeIds, boolean cache, Duration timeout) {
        ensureRunning();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        logRequest("getAttributes", streamSegmentName, attributeIds, cache);
        this.metrics.getAttributes();
        return this.metadataStore.getOrAssignSegmentId(streamSegmentName, timer.getRemaining(),
                streamSegmentId -> getAttributesForSegment(streamSegmentId, attributeIds, cache, timer));
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        ensureRunning();

        logRequest("read", streamSegmentName, offset, maxLength);
        this.metrics.read();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.metadataStore
                .getOrAssignSegmentId(streamSegmentName, timer.getRemaining(),
                        streamSegmentId -> {
                            try {
                                return CompletableFuture.completedFuture(this.readIndex.read(streamSegmentId, offset, maxLength, timer.getRemaining()));
                            } catch (StreamSegmentNotExistsException ex) {
                                return Futures.failedFuture(ex);
                            }
                        });
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("getStreamSegmentInfo", streamSegmentName);
        this.metrics.getInfo();
        return this.metadataStore.getSegmentInfo(streamSegmentName, timeout);
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, SegmentType segmentType,
                                                       Collection<AttributeUpdate> attributes, Duration timeout) {
        ensureRunning();

        logRequest("createStreamSegment", streamSegmentName, segmentType);
        this.metrics.createSegment();
        return this.metadataStore.createSegment(streamSegmentName, segmentType, attributes, timeout);
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("deleteStreamSegment", streamSegmentName);
        this.metrics.deleteSegment();
        TimeoutTimer timer = new TimeoutTimer(timeout);

        long segmentId = this.metadata.getStreamSegmentId(streamSegmentName, false);
        SegmentMetadata toDelete = this.metadata.getStreamSegmentMetadata(segmentId);
        return this.metadataStore.deleteSegment(streamSegmentName, timer.getRemaining())
                .thenAccept(deleted -> {
                    if (!deleted) {
                        // No segment to delete, which likely means Segment does not exist.
                        throw new CompletionException(new StreamSegmentNotExistsException(streamSegmentName));
                    }

                    if (toDelete != null) {
                        // Notify any internal components that this Segment is no longer part of the metadata.
                        notifyMetadataRemoved(Collections.singleton(toDelete));
                    }
                });
    }

    @Override
    public CompletableFuture<Void> truncateStreamSegment(String streamSegmentName, long offset, Duration timeout) {
        ensureRunning();
        logRequest("truncateStreamSegment", streamSegmentName);
        this.metrics.truncate();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.metadataStore.getOrAssignSegmentId(streamSegmentName, timer.getRemaining(),
                streamSegmentId -> truncate(streamSegmentId, offset, timer.getRemaining()));
    }

    @Override
    public CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(String targetStreamSegment, String sourceStreamSegment, Duration timeout) {
        ensureRunning();

        logRequest("mergeStreamSegment", targetStreamSegment, sourceStreamSegment);
        this.metrics.mergeSegment();
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // Fetch the Ids of both the source and the target Segments, then execute the MergeOperation and finally remove
        // any Segment Info about the Source Segment from the metadata. Since these operations cannot execute atomically,
        // we need to handle the case when a previous invocation completed partially: the merge executed but we were unable
        // to clear the Segment Info; in this case, upon a retry, we need to ignore the StreamSegmentMergedException and
        // complete the cleanup phase, but still bubble up any exceptions to the caller.
        return this.metadataStore
                .getOrAssignSegmentId(targetStreamSegment, timer.getRemaining(),
                        targetSegmentId -> this.metadataStore.getOrAssignSegmentId(sourceStreamSegment, timer.getRemaining(),
                                sourceSegmentId -> mergeStreamSegment(targetSegmentId, sourceSegmentId, timer)))
                .handleAsync((msr, ex) -> {
                    if (ex == null || Exceptions.unwrap(ex) instanceof StreamSegmentMergedException) {
                        // No exception or segment was already merged. Need to clear SegmentInfo for source.
                        // We can do this asynchronously and not wait on it.
                        this.metadataStore.clearSegmentInfo(sourceStreamSegment, timer.getRemaining());
                    }

                    if (ex == null) {
                        // Everything is good. Return the result.
                        return msr;
                    } else {
                        // Re-throw the exception to the caller in this case.
                        throw new CompletionException(ex);
                    }
                }, this.executor);
    }

    private CompletableFuture<MergeStreamSegmentResult> mergeStreamSegment(long targetSegmentId, long sourceSegmentId, TimeoutTimer timer) {
        // Get a reference to the source segment's metadata now, before the merge. It may not be accessible afterwards.
        SegmentMetadata sourceMetadata = this.metadata.getStreamSegmentMetadata(sourceSegmentId);

        CompletableFuture<Void> sealResult = trySealStreamSegment(sourceMetadata, timer.getRemaining());
        if (sourceMetadata.getLength() == 0) {
            // Source is empty. We may be able to skip the merge altogether and simply delete the segment. But we can only
            // be certain of this if the source is also sealed, otherwise it's possible it may still have outstanding
            // writes in the pipeline. As such, we cannot pipeline the two operations, and must wait for the seal to finish first.
            return sealResult.thenComposeAsync(v -> {
                // Seal is done. The DurableLog guarantees that the metadata is now updated with all operations up
                // to and including the seal, so if there were any writes outstanding before, they should now be reflected in it.
                if (sourceMetadata.getLength() == 0) {
                    // Source is still empty after sealing - OK to delete.
                    log.debug("{}: Deleting empty source segment instead of merging {}.", this.traceObjectId, sourceMetadata.getName());
                    return deleteStreamSegment(sourceMetadata.getName(), timer.getRemaining()).thenApply(v2 ->
                            new MergeStreamSegmentResult(this.metadata.getStreamSegmentMetadata(targetSegmentId).getLength(),
                                    sourceMetadata.getLength(), sourceMetadata.getAttributes()));
                } else {
                    // Source now has some data - we must merge the two.
                    MergeSegmentOperation operation = new MergeSegmentOperation(targetSegmentId, sourceSegmentId);
                    return addOperation(operation, timer.getRemaining()).thenApply(v2 ->
                            new MergeStreamSegmentResult(operation.getStreamSegmentOffset() + operation.getLength(),
                                    operation.getLength(), sourceMetadata.getAttributes()));
                }
            }, this.executor);
        } else {
            // Source is not empty, so we cannot delete. Make use of the DurableLog's pipelining abilities by queueing up
            // the Merge right after the Seal.
            MergeSegmentOperation operation = new MergeSegmentOperation(targetSegmentId, sourceSegmentId);
            return CompletableFuture.allOf(sealResult,
                    addOperation(operation, timer.getRemaining())).thenApply(v2 ->
                    new MergeStreamSegmentResult(operation.getStreamSegmentOffset() + operation.getLength(),
                            operation.getLength(), sourceMetadata.getAttributes()));
        }
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        ensureRunning();
        logRequest("seal", streamSegmentName);
        this.metrics.seal();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.metadataStore.getOrAssignSegmentId(streamSegmentName, timer.getRemaining(),
                streamSegmentId -> seal(streamSegmentId, timer.getRemaining()));
    }

    @Override
    public CompletableFuture<DirectSegmentAccess> forSegment(String streamSegmentName, Duration timeout) {
        ensureRunning();

        logRequest("forSegment", streamSegmentName);
        return this.metadataStore
                .getOrAssignSegmentId(streamSegmentName, timeout,
                        segmentId -> CompletableFuture.completedFuture(new DirectSegmentWrapper(segmentId)));
    }

    //endregion

    //region SegmentContainer Implementation

    @Override
    public Collection<SegmentProperties> getActiveSegments() {
        ensureRunning();
        logRequest("getActiveSegments");

        // To reduce locking in the metadata, we first get the list of Segment Ids, then we fetch their metadata
        // one by one. This only locks the metadata on the first call and, individually, on each call to getStreamSegmentMetadata.
        return this.metadata.getAllStreamSegmentIds()
                .stream()
                .map(this.metadata::getStreamSegmentMetadata)
                .filter(Objects::nonNull)
                .map(SegmentMetadata::getSnapshot)
                .collect(Collectors.toList());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends SegmentContainerExtension> T getExtension(Class<T> extensionClass) {
        SegmentContainerExtension extension = this.extensions.get(extensionClass);
        return extension == null ? null : (T) extension;
    }

    @Override
    public CompletableFuture<Void> flushToStorage(Duration timeout) {
        LogFlusher flusher = new LogFlusher(this.metadata.getContainerId(), this.durableLog, this.writer, this.metadataCleaner, this.executor);
        return flusher.flushToStorage(timeout);
    }

    //endregion

    //region Helpers

    private CompletableFuture<Void> updateAttributesForSegment(long segmentId, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
        UpdateAttributesOperation operation = new UpdateAttributesOperation(segmentId, attributeUpdates);
        return processAttributeUpdaterOperation(operation, new TimeoutTimer(timeout));
    }

    private CompletableFuture<Map<UUID, Long>> getAttributesForSegment(long segmentId, Collection<UUID> attributeIds, boolean cache, TimeoutTimer timer) {
        SegmentMetadata metadata = this.metadata.getStreamSegmentMetadata(segmentId);
        if (cache) {
            return CACHE_ATTRIBUTES_RETRY.runAsync(() ->
                    getAndCacheAttributes(metadata, attributeIds, cache, timer), StreamSegmentContainer.this.executor);
        } else {
            return getAndCacheAttributes(metadata, attributeIds, cache, timer);
        }
    }

    private CompletableFuture<Long> seal(long segmentId, Duration timeout) {
        StreamSegmentSealOperation operation = new StreamSegmentSealOperation(segmentId);
        return addOperation(operation, timeout)
                .thenApply(seqNo -> operation.getStreamSegmentOffset());
    }

    private CompletableFuture<Void> truncate(long segmentId, long offset, Duration timeout) {
        StreamSegmentTruncateOperation op = new StreamSegmentTruncateOperation(segmentId, offset);
        return addOperation(op, timeout);
    }

    /**
     * Attempts to seal a Segment that may already be sealed.
     *
     * @param metadata The SegmentMetadata for the Segment to Seal.
     * @param timeout  Timeout for the operation.
     * @return A CompletableFuture that will indicate when the operation completes. If the given segment is already sealed,
     * this future will already be completed, otherwise it will complete once the seal is performed.
     */
    private CompletableFuture<Void> trySealStreamSegment(SegmentMetadata metadata, Duration timeout) {
        if (metadata.isSealed()) {
            return CompletableFuture.completedFuture(null);
        } else {
            // It is OK to ignore StreamSegmentSealedException as the segment may have already been sealed by a concurrent
            // call to this or via some other operation.
            return Futures.exceptionallyExpecting(
                    addOperation(new StreamSegmentSealOperation(metadata.getId()), timeout),
                    ex -> ex instanceof StreamSegmentSealedException,
                    null);
        }
    }

    /**
     * Processes the given {@link StreamSegmentAppendOperation} and ensures that the {@link StreamSegmentAppendOperation#close()}
     * is invoked in case the operation failed to process (for whatever reason). If the operation completed successfully,
     * the {@link OperationLog} will close it internally when it finished any async processing with it.
     *
     * @param appendOperation The Operation to process.
     * @param timer           Timer for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate that the Operation has been successfully
     * processed. If it failed, it will be completed with an appropriate exception.
     */
    private CompletableFuture<Void> processAppend(StreamSegmentAppendOperation appendOperation, TimeoutTimer timer) {
        CompletableFuture<Void> result = processAttributeUpdaterOperation(appendOperation, timer);
        Futures.exceptionListener(result, ex -> appendOperation.close());
        return result;
    }

    /**
     * Processes the given AttributeUpdateOperation with exactly one retry in case it was rejected because of an attribute
     * update failure due to the attribute value missing from the in-memory cache.
     *
     * @param operation The Operation to process.
     * @param timer     Timer for the operation.
     * @param <T>       Type of the operation.
     * @return A CompletableFuture that, when completed normally, will indicate that the Operation has been successfully
     * processed. If it failed, it will be completed with an appropriate exception.
     */
    private <T extends Operation & AttributeUpdaterOperation> CompletableFuture<Void> processAttributeUpdaterOperation(T operation, TimeoutTimer timer) {
        Collection<AttributeUpdate> updates = operation.getAttributeUpdates();
        if (updates == null || updates.isEmpty()) {
            // No need for extra complicated handling.
            return addOperation(operation, timer.getRemaining());
        }

        return Futures.exceptionallyCompose(
                addOperation(operation, timer.getRemaining()),
                ex -> {
                    // We only retry BadAttributeUpdateExceptions if it has the PreviousValueMissing flag set.
                    ex = Exceptions.unwrap(ex);
                    if (ex instanceof BadAttributeUpdateException && ((BadAttributeUpdateException) ex).isPreviousValueMissing()) {
                        // Get the missing attributes and load them into the cache, then retry the operation, exactly once.
                        SegmentMetadata segmentMetadata = this.metadata.getStreamSegmentMetadata(operation.getStreamSegmentId());
                        Collection<UUID> attributeIds = updates.stream()
                                .map(AttributeUpdate::getAttributeId)
                                .filter(id -> !Attributes.isCoreAttribute(id))
                                .collect(Collectors.toList());
                        if (!attributeIds.isEmpty()) {
                            // This only makes sense if a core attribute was missing.
                            return getAndCacheAttributes(segmentMetadata, attributeIds, true, timer)
                                    .thenComposeAsync(attributes -> {
                                        // Final attempt - now that we should have the attributes cached.
                                        return addOperation(operation, timer.getRemaining());
                                    }, this.executor);
                        }
                    }

                    // Anything else is non-retryable; rethrow.
                    return Futures.failedFuture(ex);
                });
    }

    /**
     * Gets the values of the given (Core and Extended) Attribute Ids for the given segment.
     *
     * @param segmentMetadata The SegmentMetadata for the Segment to retrieve attribute values for.
     * @param attributeIds    A Collection of AttributeIds to retrieve.
     * @param cache           If true, any Extended Attribute value that is not present in the SegmentMetadata cache will
     *                        be added to that (using a conditional updateAttributes() call) before completing.
     * @param timer           Timer for the operation.
     * @return A CompletableFuture that, when completed normally, will contain the desired result. If the operation failed,
     * it will be completed with the appropriate exception. If cache==true and the conditional call to updateAttributes()
     * could not be completed because of a conflicting update, it will be failed with BadAttributeUpdateException, in which
     * case a retry is warranted.
     */
    private CompletableFuture<Map<UUID, Long>> getAndCacheAttributes(SegmentMetadata segmentMetadata, Collection<UUID> attributeIds, boolean cache, TimeoutTimer timer) {
        // Collect Core Attributes and Cached Extended Attributes.
        Map<UUID, Long> result = new HashMap<>();
        Map<UUID, Long> metadataAttributes = segmentMetadata.getAttributes();
        ArrayList<UUID> extendedAttributeIds = new ArrayList<>();
        attributeIds.forEach(attributeId -> {
            Long v = metadataAttributes.get(attributeId);
            if (v != null) {
                // This attribute is cached in the Segment Metadata, even if it has a value equal to Attributes.NULL_ATTRIBUTE_VALUE.
                result.put(attributeId, v);
            } else if (!Attributes.isCoreAttribute(attributeId)) {
                extendedAttributeIds.add(attributeId);
            }
        });

        if (extendedAttributeIds.isEmpty()) {
            // Nothing to lookup in the Attribute Index, so bail out early.
            return CompletableFuture.completedFuture(result);
        }

        // Collect remaining Extended Attributes.
        CompletableFuture<Map<UUID, Long>> r = this.attributeIndex
                .forSegment(segmentMetadata.getId(), timer.getRemaining())
                .thenComposeAsync(idx -> idx.get(extendedAttributeIds, timer.getRemaining()), this.executor)
                .thenApplyAsync(extendedAttributes -> {
                    if (extendedAttributeIds.size() == extendedAttributes.size()) {
                        // We found a value for each Attribute Id. Nothing more to do.
                        return extendedAttributes;
                    }

                    // Insert a NULL_ATTRIBUTE_VALUE for each missing value.
                    Map<UUID, Long> allValues = new HashMap<>(extendedAttributes);
                    extendedAttributeIds.stream()
                                        .filter(id -> !extendedAttributes.containsKey(id))
                                        .forEach(id -> allValues.put(id, Attributes.NULL_ATTRIBUTE_VALUE));
                    return allValues;
                }, this.executor);

        if (cache && !segmentMetadata.isSealed()) {
            // Add them to the cache if requested.
            r = r.thenComposeAsync(extendedAttributes -> {
                // Update the in-memory Segment Metadata using a special update (AttributeUpdateType.None, which should
                // complete if the attribute is not currently set). If it has some value, then a concurrent update
                // must have changed it and we cannot update anymore.
                List<AttributeUpdate> updates = extendedAttributes
                        .entrySet().stream()
                        .map(e -> new AttributeUpdate(e.getKey(), AttributeUpdateType.None, e.getValue()))
                        .collect(Collectors.toList());

                // We need to make sure not to update attributes via updateAttributes() as that method may indirectly
                // invoke this one again.
                return addOperation(new UpdateAttributesOperation(segmentMetadata.getId(), updates), timer.getRemaining())
                        .thenApply(v -> extendedAttributes);
            }, this.executor);
        }

        // Compile the final result.
        return r.thenApply(extendedAttributes -> {
            result.putAll(extendedAttributes);
            return result;
        });
    }

    private CompletableFuture<AttributeIterator> attributeIterator(long segmentId, UUID fromId, UUID toId, Duration timeout) {
        return this.attributeIndex.forSegment(segmentId, timeout)
                .thenApplyAsync(index -> {
                    AttributeIterator indexIterator = index.iterator(fromId, toId, timeout);
                    return new SegmentAttributeIterator(indexIterator, this.metadata.getStreamSegmentMetadata(segmentId), fromId, toId);
                }, this.executor);
    }

    /**
     * Callback that notifies eligible components that the given Segments' metadatas has been removed from the metadata,
     * regardless of the trigger (eviction or deletion).
     *
     * @param segments A Collection of SegmentMetadatas for those segments which were removed.
     */
    protected void notifyMetadataRemoved(Collection<SegmentMetadata> segments) {
        if (segments.size() > 0) {
            Collection<Long> segmentIds = segments.stream().map(SegmentMetadata::getId).collect(Collectors.toList());
            this.readIndex.cleanup(segmentIds);
            this.attributeIndex.cleanup(segmentIds);
        }
    }

    private void ensureRunning() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        if (state() != State.RUNNING) {
            throw new IllegalContainerStateException(this.getId(), state(), State.RUNNING);
        } else if (isOffline()) {
            throw new ContainerOfflineException(getId());
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

    private CompletableFuture<Void> runMetadataCleanup() {
        return this.metadataCleaner.runOnce();
    }

    private CompletableFuture<Long> mapSegmentId(long segmentId, SegmentProperties segmentProperties, boolean pin, Duration timeout) {
        StreamSegmentMapOperation op = new StreamSegmentMapOperation(segmentProperties);
        if (segmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID) {
            op.setStreamSegmentId(segmentId);
        }

        if (pin) {
            op.markPinned();
        }

        OperationPriority priority = calculatePriority(SegmentType.fromAttributes(segmentProperties.getAttributes()), op.getType());
        return this.durableLog.add(op, priority, timeout).thenApply(ignored -> op.getStreamSegmentId());
    }

    private CompletableFuture<Void> deleteSegmentImmediate(String segmentName, Duration timeout) {
        // Delete both the Segment and its Attribute Index in parallel. This handles the case when the segment file does
        // not exist (no data appended) but it does have attributes.
        return CompletableFuture.allOf(
                this.storage
                        .openWrite(segmentName)
                        .thenComposeAsync(handle -> this.storage.delete(handle, timeout), this.executor),
                this.attributeIndex.delete(segmentName, timeout));
    }

    private CompletableFuture<Void> deleteSegmentDelayed(long segmentId, Duration timeout) {
        // NOTE: DeleteSegmentOperations have their OperationPriority set to Critical.
        return addOperation(new DeleteSegmentOperation(segmentId), timeout);
    }

    private <T extends Operation & SegmentOperation> CompletableFuture<Void> addOperation(T operation, Duration timeout) {
        SegmentMetadata sm = this.metadata.getStreamSegmentMetadata(operation.getStreamSegmentId());
        OperationPriority priority = calculatePriority(sm.getType(), operation.getType());
        return this.durableLog.add(operation, priority, timeout);
    }

    private OperationPriority calculatePriority(SegmentType segmentType, OperationType operationType) {
        return PriorityCalculator.getPriority(segmentType, operationType);
    }

    //endregion

    //region DirectSegmentWrapper

    /**
     * Direct Segment Access implementation.
     */
    @RequiredArgsConstructor
    private class DirectSegmentWrapper implements DirectSegmentAccess {
        @Getter
        private final long segmentId;

        @Override
        public CompletableFuture<Long> append(BufferView data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            ensureRunning();
            logRequest("append", this.segmentId, data.getLength());
            StreamSegmentAppendOperation operation = new StreamSegmentAppendOperation(this.segmentId, data, attributeUpdates);
            return processAppend(operation, new TimeoutTimer(timeout))
                    .thenApply(v -> operation.getStreamSegmentOffset());
        }

        @Override
        public CompletableFuture<Long> append(BufferView data, Collection<AttributeUpdate> attributeUpdates, long offset, Duration timeout) {
            ensureRunning();
            logRequest("append", this.segmentId, data.getLength());
            StreamSegmentAppendOperation operation = new StreamSegmentAppendOperation(this.segmentId, offset, data, attributeUpdates);
            return processAppend(operation, new TimeoutTimer(timeout))
                    .thenApply(v -> operation.getStreamSegmentOffset());
        }

        @Override
        public CompletableFuture<Void> updateAttributes(Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            ensureRunning();
            logRequest("updateAttributes", this.segmentId, attributeUpdates);
            return StreamSegmentContainer.this.updateAttributesForSegment(this.segmentId, attributeUpdates, timeout);
        }

        @Override
        public CompletableFuture<Map<UUID, Long>> getAttributes(Collection<UUID> attributeIds, boolean cache, Duration timeout) {
            ensureRunning();
            logRequest("getAttributes", this.segmentId, attributeIds, cache);
            return StreamSegmentContainer.this.getAttributesForSegment(this.segmentId, attributeIds, cache, new TimeoutTimer(timeout));
        }

        @Override
        @SneakyThrows(StreamSegmentNotExistsException.class)
        public ReadResult read(long offset, int maxLength, Duration timeout) {
            ensureRunning();
            logRequest("read", this.segmentId, offset, maxLength);
            return StreamSegmentContainer.this.readIndex.read(this.segmentId, offset, maxLength, timeout);
        }

        @Override
        public SegmentProperties getInfo() {
            ensureRunning();
            return StreamSegmentContainer.this.metadata.getStreamSegmentMetadata(this.segmentId);
        }

        @Override
        public CompletableFuture<Long> seal(Duration timeout) {
            ensureRunning();
            logRequest("seal", this.segmentId);
            return StreamSegmentContainer.this.seal(this.segmentId, timeout);
        }

        @Override
        public CompletableFuture<Void> truncate(long offset, Duration timeout) {
            ensureRunning();
            logRequest("truncateStreamSegment", this.segmentId);
            return StreamSegmentContainer.this.truncate(this.segmentId, offset, timeout);
        }

        @Override
        public CompletableFuture<AttributeIterator> attributeIterator(UUID fromId, UUID toId, Duration timeout) {
            ensureRunning();
            logRequest("attributeIterator", this.segmentId, fromId, toId);
            return StreamSegmentContainer.this.attributeIterator(this.segmentId, fromId, toId, timeout);
        }
    }

    //endregion
}
