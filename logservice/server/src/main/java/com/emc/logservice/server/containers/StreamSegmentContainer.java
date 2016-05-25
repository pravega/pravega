package com.emc.logservice.server.containers;

import com.emc.logservice.storageabstraction.Storage;
import com.emc.logservice.storageabstraction.StorageFactory;
import com.emc.logservice.contracts.*;
import com.emc.logservice.server.*;
import com.emc.logservice.server.core.AsyncLock;
import com.emc.logservice.server.core.TimeoutTimer;
import com.emc.logservice.server.logs.OperationLog;
import com.emc.logservice.server.logs.operations.MergeBatchOperation;
import com.emc.logservice.server.logs.operations.StreamSegmentAppendOperation;

import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Container for StreamSegments. All StreamSegments that are related (based on a hashing functions) will belong to the
 * same StreamSegmentContainer. Handles all operations that can be performed on such streams.
 */
public class StreamSegmentContainer implements StreamSegmentStore, Container {

    private static final Duration CloseTimeout = Duration.ofSeconds(30); //TODO: make configurable
    private final UpdateableContainerMetadata metadata;
    private final OperationLog durableLog;
    private final Cache readIndex;
    private final Storage storage;
    private final StreamSegmentMapper segmentMapper;
    private final FaultHandlerRegistry faultRegistry;
    private final AsyncLock StateTransitionLock = new AsyncLock();
    private ContainerState state;
    private boolean closed;

    /**
     * Creates a new instance of the StreamSegmentContainer class.
     *
     * @param streamSegmentContainerId
     * @param metadataRepository
     * @param durableLogFactory
     * @param cacheFactory
     */
    public StreamSegmentContainer(String streamSegmentContainerId, MetadataRepository metadataRepository, OperationLogFactory durableLogFactory, CacheFactory cacheFactory, StorageFactory storageFactory) {
        this.faultRegistry = new FaultHandlerRegistry();
        this.storage = storageFactory.getStorageAdapter();
        this.metadata = metadataRepository.getMetadata(streamSegmentContainerId);
        this.readIndex = cacheFactory.createCache(this.metadata);
        this.durableLog = durableLogFactory.createDurableLog(metadata, readIndex);
        this.durableLog.registerFaultHandler(this.faultRegistry::handle);
        this.segmentMapper = new StreamSegmentMapper(this.metadata, this.durableLog, this.storage);
        setState(ContainerState.Created);
    }

    //region AutoCloseable Implementation

    @Override
    public void close() throws Exception {
        if (!this.closed) {
            if (this.state == ContainerState.Started) {
                // Stop the container if it's already running.
                stop(CloseTimeout).get();
            }

            this.closed = true;
        }
    }

    //endregion

    //region Container Implementation

    @Override
    public CompletableFuture<Void> initialize(Duration timeout) {
        ensureNotClosed();
        return this.StateTransitionLock.execute(() -> {
            ContainerState.Initialized.checkValidPreviousState(this.state);

            return this.durableLog.initialize(timeout) // Initialize DurableLog.
                                  .thenRun(() -> setState(ContainerState.Initialized)); // Update our internal state.
        });
    }

    @Override
    public CompletableFuture<Void> start(Duration timeout) {
        ensureNotClosed();
        return this.StateTransitionLock.execute(() ->
        {
            ContainerState.Started.checkValidPreviousState(this.state);

            // Start the Operation Queue Processor.
            return this.durableLog.start(timeout)
                                  .thenRun(() -> setState(ContainerState.Started));
        });
    }

    @Override
    public CompletableFuture<Void> stop(Duration timeout) {
        ensureNotClosed();

        return this.StateTransitionLock.execute(() ->
        {
            ContainerState.Stopped.checkValidPreviousState(this.state);

            // Update the state first. TODO: figure out if we need to roll back the state if this operation failed.
            setState(ContainerState.Stopped);

            // Stop the Operation Queue Processor. TODO: should we also stop the read index? Or are the checks in this class enough?
            return this.durableLog.stop(timeout);
        });
    }

    @Override
    public void registerFaultHandler(Consumer<Throwable> handler) {
        ensureNotClosed();
    }

    @Override
    public ContainerState getState() {
        return this.state;
    }

    @Override
    public String getId() {
        return this.metadata.getContainerId();
    }

    //endregion

    //region StreamSegmentStore Implementation

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, byte[] data, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                                 .thenCompose(streamSegmentId ->
                                 {
                                     com.emc.logservice.server.logs.operations.Operation operation = new StreamSegmentAppendOperation(streamSegmentId, data);
                                     return this.durableLog.add(operation, timer.getRemaining());
                                 });
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                                 .thenApply(streamSegmentId -> this.readIndex.read(streamSegmentId, offset, maxLength, timer.getRemaining()));
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                                 .thenApply(streamSegmentId ->
                                 {
                                     SegmentMetadata sm = this.metadata.getStreamSegmentMetadata(streamSegmentId);
                                     return new StreamSegmentInformation(streamSegmentName, sm.getDurableLogLength(), sm.isSealed(), sm.isDeleted(), new Date());
                                 });
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.createNewStreamSegment(streamSegmentName, timer.getRemaining());
    }

    @Override
    public CompletableFuture<String> createBatch(String parentStreamName, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.createNewBatchStreamSegment(parentStreamName, timer.getRemaining());
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);
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
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.getOrAssignStreamSegmentId(batchStreamSegmentName, timer.getRemaining())
                                 .thenCompose(batchStreamSegmentId ->
                                 {
                                     SegmentMetadata batchMetadata = this.metadata.getStreamSegmentMetadata(batchStreamSegmentId);
                                     if (batchMetadata == null) {
                                         throw new CompletionException(new StreamingException("Batch StreamSegment does not exist."));
                                     }

                                     com.emc.logservice.server.logs.operations.Operation op = new MergeBatchOperation(batchMetadata.getParentId(), batchMetadata.getId());
                                     return this.durableLog.add(op, timer.getRemaining());
                                 });
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                                 .thenCompose(streamSegmentId ->
                                 {
                                     com.emc.logservice.server.logs.operations.Operation operation = new com.emc.logservice.server.logs.operations.StreamSegmentSealOperation(streamSegmentId);
                                     return this.durableLog.add(operation, timer.getRemaining());
                                 });
    }

    //endregion

    //region Helpers

    private void ensureStarted() {
        ensureNotClosed();
        if (this.state != ContainerState.Started) {
            throw new com.emc.logservice.server.core.ObjectClosedException(this);
        }
    }

    private void ensureNotClosed() {
        if (this.closed) {
            throw new com.emc.logservice.server.core.ObjectClosedException(this);
        }
    }

    private void setState(ContainerState state) {
        this.state = state;
    }

    //endregion
}
