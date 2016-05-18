package com.emc.logservice;

import com.emc.logservice.core.*;
import com.emc.logservice.logs.*;
import com.emc.logservice.logs.operations.*;
import com.emc.logservice.mocks.InMemoryDataFrameLog;
import com.emc.logservice.reading.ReadIndex;

import java.time.Duration;
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
    private final StreamSegmentContainerMetadata metadata;
    private final OperationLog durableLog;
    private final DataFrameLog dataFrameLog;
    private final StreamSegmentCache cache;
    private final StreamSegmentMapper segmentMapper;
    private final FaultHandlerRegistry faultRegistry;
    private final AsyncLock StateTransitionLock = new AsyncLock();
    private ContainerState state;
    private boolean closed;

    public StreamSegmentContainer() {
        //TODO: figure out how to construct and read metadata from the Metadata Storage.
        this.metadata = new StreamSegmentContainerMetadata();

        // TODO: accept some sort of Factory that provides all the necessary components.
        this.faultRegistry = new FaultHandlerRegistry();
        this.dataFrameLog = new InMemoryDataFrameLog();
        this.cache = new ReadIndex(this.metadata);
        this.durableLog = new DurableLog(metadata, dataFrameLog, cache);
        this.durableLog.registerFaultHandler(this.faultRegistry::handle);

        this.segmentMapper = new StreamSegmentMapper(this.metadata, this.durableLog);
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

    //endregion

    //region StreamSegmentStore Implementation

    @Override
    public CompletableFuture<Long> append(String streamSegmentName, byte[] data, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                                 .thenCompose(streamSegmentId ->
                                 {
                                     Operation operation = new StreamSegmentAppendOperation(streamSegmentId, data);
                                     return this.durableLog.add(operation, timer.getRemaining());
                                 });
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                                 .thenApply(streamSegmentId -> this.cache.read(streamSegmentId, offset, maxLength, timer.getRemaining()));
    }

    @Override
    public CompletableFuture<StreamSegmentInformation> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.getOrAssignStreamSegmentId(streamSegmentName, timer.getRemaining())
                                 .thenApply(streamSegmentId ->
                                 {
                                     StreamSegmentMetadata sm = this.metadata.getStreamSegmentMetadata(streamSegmentId);
                                     return new StreamSegmentInformation(streamSegmentName, sm.getDurableLogLength(), sm.isSealed(), sm.isDeleted(), new Date());
                                 });
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);

        //TODO: implement
        return null;
    }

    @Override
    public CompletableFuture<String> createBatch(String parentStreamName, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);

        //TODO: implement
        return null;
    }

    @Override
    public CompletableFuture<Long> mergeBatch(String batchStreamSegmentName, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.segmentMapper.getOrAssignStreamSegmentId(batchStreamSegmentName, timer.getRemaining())
                                 .thenCompose(batchStreamSegmentId ->
                                 {
                                     StreamSegmentMetadata batchMetadata = this.metadata.getStreamSegmentMetadata(batchStreamSegmentId);
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
                                     Operation operation = new StreamSegmentSealOperation(streamSegmentId);
                                     return this.durableLog.add(operation, timer.getRemaining());
                                 });
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamName, Duration timeout) {
        ensureStarted();

        TimeoutTimer timer = new TimeoutTimer(timeout);

        //TODO: implement
        return null;
    }

    //endregion

    //region Helpers

    private void ensureStarted() {
        ensureNotClosed();
        if (this.state != ContainerState.Started) {
            throw new ObjectClosedException(this);
        }
    }

    private void ensureNotClosed() {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }
    }

    private void setState(ContainerState state) {
        this.state = state;
    }

    //endregion
}
