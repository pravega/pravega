package com.emc.logservice.server.containers;

import com.emc.logservice.common.*;
import com.emc.logservice.contracts.*;
import com.emc.logservice.server.*;
import com.emc.logservice.server.logs.OperationLog;
import com.emc.logservice.server.logs.PendingAppendsCollection;
import com.emc.logservice.server.logs.operations.*;
import com.emc.logservice.storageabstraction.Storage;
import com.emc.logservice.storageabstraction.StorageFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Container for StreamSegments. All StreamSegments that are related (based on a hashing functions) will belong to the
 * same StreamSegmentContainer. Handles all operations that can be performed on such streams.
 */
class StreamSegmentContainer implements SegmentContainer {
    //region Members

    private static final Duration CloseTimeout = Duration.ofSeconds(30); //TODO: make configurable
    private final UpdateableContainerMetadata metadata;
    private final OperationLog durableLog;
    private final Cache readIndex;
    private final Storage storage;
    private final PendingAppendsCollection pendingAppendsCollection;
    private final StreamSegmentMapper segmentMapper;
    private final FaultHandlerRegistry faultRegistry;
    private final AsyncLock StateTransitionLock = new AsyncLock();
    private ContainerState state;

    //endregion

    //region Constructor

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
        this.pendingAppendsCollection = new PendingAppendsCollection();
        this.segmentMapper = new StreamSegmentMapper(this.metadata, this.durableLog, this.storage);
        setState(ContainerState.Created);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.state != ContainerState.Closed) {
            if (this.state == ContainerState.Started) {
                // Stop the container if it's already running.
                stop(CloseTimeout).join();
            }

            this.pendingAppendsCollection.close();
            this.durableLog.close();
            this.readIndex.close();
            setState(ContainerState.Closed);
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

            // Update the state first.
            setState(ContainerState.Stopped);

            // Stop the Operation Queue Processor.
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
    public CompletableFuture<Long> append(String streamSegmentName, byte[] data, AppendContext appendContext, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
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

                                     Operation op = new MergeBatchOperation(batchMetadata.getParentId(), batchMetadata.getId());
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
                                     Operation operation = new com.emc.logservice.server.logs.operations.StreamSegmentSealOperation(streamSegmentId);
                                     return this.durableLog.add(operation, timer.getRemaining());
                                 });
    }

    @Override
    public CompletableFuture<AppendContext> getLastAppendContext(String streamSegmentName, UUID clientId) {
        ensureStarted();

        long streamSegmentId = this.metadata.getStreamSegmentId(streamSegmentName);
        if (streamSegmentId == StreamSegmentContainerMetadata.NoStreamSegmentId) {
            // We do not have any recent information about this StreamSegment. Do not bother to create an entry with it using SegmentMapper.
            return null;
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

    private void ensureStarted() {
        ensureNotClosed();
        if (this.state != ContainerState.Started) {
            throw new IllegalContainerStateException(this.getId(), this.state, ContainerState.Started);
        }
    }

    private void ensureNotClosed() {
        if (this.state == ContainerState.Closed) {
            throw new ObjectClosedException(this);
        }
    }

    private void setState(ContainerState state) {
        this.state = state;
    }

    //endregion
}
