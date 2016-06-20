package com.emc.logservice.server.logs;

import com.emc.logservice.common.*;
import com.emc.logservice.server.*;
import com.emc.logservice.server.containers.TruncationMarkerCollection;
import com.emc.logservice.server.logs.operations.*;
import com.emc.logservice.storageabstraction.*;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Represents an OperationLog that durably stores Log Operations it receives.
 */
@Slf4j
public class DurableLog implements OperationLog {
    //region Members

    private static final Duration CloseTimeout = Duration.ofSeconds(30); // TODO: make configurable.
    private final String traceObjectId;
    private final LogItemFactory<Operation> operationFactory;
    private final MemoryOperationLog inMemoryOperationLog;
    private final DurableDataLog dataFrameLog;
    private final MemoryLogUpdater memoryLogUpdater;
    private final OperationQueue queue;
    private final OperationQueueProcessor queueProcessor;
    private final UpdateableContainerMetadata metadata;
    private final TruncationMarkerCollection truncationMarkers;
    private final FaultHandlerRegistry faultRegistry;
    private final AsyncLock StateTransitionLock = new AsyncLock();
    private ContainerState state;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DurableLog class.
     *
     * @param metadata            The StreamSegment Container Metadata for the container which this Durable Log is part of.
     * @param dataFrameLogFactory A DurableDataLogFactory which can be used to create instances of DataFrameLogs.
     * @param cache               An Cache where to store newly processed appends.
     * @throws NullPointerException If any of the arguments are null.
     */
    public DurableLog(UpdateableContainerMetadata metadata, DurableDataLogFactory dataFrameLogFactory, Cache cache) {
        Exceptions.throwIfNull(metadata, "metadata");
        Exceptions.throwIfNull(dataFrameLogFactory, "dataFrameLogFactory");
        Exceptions.throwIfNull(cache, "cache");

        this.dataFrameLog = dataFrameLogFactory.createDurableDataLog(metadata.getContainerId());
        assert this.dataFrameLog != null : "dataFrameLogFactory created null dataFrameLog.";

        this.traceObjectId = String.format("DurableLog[%s]", metadata.getContainerId());
        this.metadata = metadata;
        this.operationFactory  = new OperationFactory();
        this.truncationMarkers = new TruncationMarkerCollection();
        this.faultRegistry = new FaultHandlerRegistry();
        this.inMemoryOperationLog = new MemoryOperationLog();
        this.memoryLogUpdater = new MemoryLogUpdater(this.inMemoryOperationLog, cache);
        this.queue = new OperationQueue();
        this.queueProcessor = new OperationQueueProcessor(this.metadata.getContainerId(), this.queue, new OperationMetadataUpdater(this.metadata, this.truncationMarkers), this.memoryLogUpdater, this.dataFrameLog);
        this.queueProcessor.registerFaultHandler(this.faultRegistry::handle);
        setState(ContainerState.Created);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.state != ContainerState.Closed) {
            if (this.state == ContainerState.Started) {
                // Stop the container if it's currently running.
                this.stop(CloseTimeout).join();
            }

            this.queueProcessor.close();
            this.dataFrameLog.close();
            setState(ContainerState.Closed);
        }
    }

    //endregion

    //region Container Implementation

    @Override
    public CompletableFuture<Void> initialize(Duration timeout) {
        TimeoutTimer timer = new TimeoutTimer(timeout);
        ensureNotClosed();
        return this.StateTransitionLock.execute(() ->
        {
            int traceId = LoggerHelpers.traceEnter(log, traceObjectId, "initialize");
            ContainerState.Initialized.checkValidPreviousState(this.state);

            // Perform Recovery and initialize all components.
            return this
                    .performRecovery(timer.getRemaining()) // Perform Recovery.
                    .thenRun(() -> this.queueProcessor.initialize(timer.getRemaining())) // Initialize Queue Processor.
                    .thenRun(() -> setState(ContainerState.Initialized)) // Update our internal state.
                    .thenRun(() -> LoggerHelpers.traceLeave(log, traceObjectId, "initialize", traceId));
        });
    }

    @Override
    public CompletableFuture<Void> start(Duration timeout) {
        ensureNotClosed();
        return this.StateTransitionLock.execute(() ->
        {
            int traceId = LoggerHelpers.traceEnter(log, traceObjectId, "start");
            ContainerState.Started.checkValidPreviousState(this.state);
            return this.queueProcessor
                    .start(timeout)
                    .thenRun(() -> setState(ContainerState.Started))
                    .thenRun(() -> LoggerHelpers.traceLeave(log, traceObjectId, "start", traceId));
        });
    }

    @Override
    public CompletableFuture<Void> stop(Duration timeout) {
        ensureNotClosed();
        return this.StateTransitionLock.execute(() ->
        {
            int traceId = LoggerHelpers.traceEnter(log, traceObjectId, "stop");
            ContainerState.Stopped.checkValidPreviousState(this.state);

            // Update the state first.
            setState(ContainerState.Stopped);

            // Stop the Operation Queue Processor.
            return this.queueProcessor
                    .stop(timeout)
                    .thenRun(() -> LoggerHelpers.traceLeave(log, traceObjectId, "stop", traceId));
        });
    }

    @Override
    public void registerFaultHandler(Consumer<Throwable> handler) {
        this.faultRegistry.register(handler);
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

    //region OperationLog Implementation

    @Override
    public CompletableFuture<Long> add(Operation operation, Duration timeout) {
        ensureStarted();
        CompletableFuture<Long> result = new CompletableFuture<>();

        // Add to queue.
        log.debug("{}: AddToQueue {}.", this.traceObjectId, operation);
        this.queue.add(new CompletableOperation(operation, result));
        return result;
    }

    @Override
    public CompletableFuture<Void> truncate(long upToSequenceNumber, Duration timeout) {
        ensureStarted();
        long dataFrameSeqNo = this.truncationMarkers.getClosestTruncationMarker(upToSequenceNumber);
        if (dataFrameSeqNo < 0) {
            // Nothing to truncate.
            return CompletableFuture.completedFuture(null);
        }

        TimeoutTimer timer = new TimeoutTimer(timeout);
        log.info("{}: Truncate (OperationSequenceNumber = {}, DataFrameSequenceNumber = {}).", this.traceObjectId, upToSequenceNumber, dataFrameSeqNo);
        return this.dataFrameLog
                .truncate(dataFrameSeqNo, timer.getRemaining()) // Truncate DataFrameLog.
                .thenApply(r -> this.inMemoryOperationLog.truncate(e -> e.getSequenceNumber() <= upToSequenceNumber)) // Truncate InMemory Transaction Log.
                .thenRun(() -> this.truncationMarkers.removeTruncationMarkers(upToSequenceNumber)); // Remove old truncation markers.
    }

    @Override
    public CompletableFuture<Iterator<Operation>> read(long afterSequenceNumber, int maxCount, Duration timeout) {
        ensureStarted();
        log.debug("{}: Read (AfterSequenceNumber = {}, MaxCount = {}).", this.traceObjectId, afterSequenceNumber, maxCount);
        return CompletableFuture.completedFuture(this.inMemoryOperationLog.read(e -> e.getSequenceNumber() > afterSequenceNumber, maxCount));
    }

    //endregion

    //region Recovery

    private CompletableFuture<Void> performRecovery(Duration timeout) {
        int traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "performRecovery");

        // Make sure we are in the correct state. We do not want to do recovery while we are in full swing.
        ContainerState.Initialized.checkValidPreviousState(this.state);

        TimeoutTimer timer = new TimeoutTimer(timeout);
        log.info("{} Recovery started.", this.traceObjectId);

        // Put metadata (and entire container) into 'Recovery Mode'.
        this.metadata.enterRecoveryMode();
        this.truncationMarkers.enterRecoveryMode();

        // Reset metadata.
        this.metadata.reset();
        this.truncationMarkers.reset();

        OperationMetadataUpdater metadataUpdater = new OperationMetadataUpdater(this.metadata, this.truncationMarkers);
        this.memoryLogUpdater.enterRecoveryMode(metadataUpdater);

        CompletableFuture<Void> result = this.dataFrameLog
                .initialize(timer.getRemaining()) // Initialize DataFrameLog.
                .thenRun(() ->
                {
                    // Recover from DataFrameLog.
                    try {
                        recoverFromDataFrameLog(metadataUpdater, timer.getRemaining());
                    }
                    catch (DurableDataLogException | DataCorruptionException ex) {
                        throw new CompletionException(ex);
                    }
                });

        // No need for error handling here. Any errors will be handles upstream, by whomever listens to our result.
        // We must exit recovery mode when done, regardless of outcome.
        result.whenComplete((r, ex) ->
        {
            this.metadata.exitRecoveryMode();
            this.truncationMarkers.exitRecoveryMode();
            this.memoryLogUpdater.exitRecoveryMode(this.metadata, ex == null);
            if (ex == null) {
                log.info("{} Recovery completed.", this.traceObjectId);
            }
            else {
                log.error("{} Recovery FAILED. {}", this.traceObjectId, ex);
            }
        });

        return result
                .thenRun(() -> LoggerHelpers.traceLeave(log, this.traceObjectId, "performRecovery", traceId));
    }

    private void recoverFromDataFrameLog(OperationMetadataUpdater metadataUpdater, Duration timeout) throws DataCorruptionException, DurableDataLogException {
        int traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "recoverFromDataFrameLog");
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // Read all entries from the DataFrameLog and append them to the InMemoryOperationLog.
        // Also update metadata along the way.
        try (DataFrameReader<Operation> reader = new DataFrameReader<>(this.dataFrameLog, this.operationFactory, getId())) {
            DataFrameReader.ReadResult lastReadResult = null;
            while (true) {
                // Fetch the next operation.
                DataFrameReader.ReadResult<Operation> readResult = reader.getNext(timer.getRemaining()).join();
                if (readResult == null) {
                    // We have reached the end.
                    break;
                }

                Operation operation = readResult.getItem();

                // Update Metadata Sequence Number.
                this.metadata.setOperationSequenceNumber(operation.getSequenceNumber());

                // Determine Truncation Markers.
                if (readResult.isLastFrameEntry()) {
                    // The current Log Operation was the last one in the frame. This is a Truncation Marker.
                    metadataUpdater.recordTruncationMarker(operation.getSequenceNumber(), readResult.getDataFrameSequence());
                }
                else if (lastReadResult != null && !lastReadResult.isLastFrameEntry() && readResult.getDataFrameSequence() != lastReadResult.getDataFrameSequence()) {
                    // DataFrameSequence changed on this operation (and this operation spans multiple frames). The Truncation Marker is on this operation, but the previous frame.
                    metadataUpdater.recordTruncationMarker(operation.getSequenceNumber(), lastReadResult.getDataFrameSequence());
                }

                lastReadResult = readResult;

                // Process the operation.
                try {
                    log.debug("{} Recovering {}.", this.traceObjectId, operation);
                    if (operation instanceof MetadataOperation) {
                        metadataUpdater.processMetadataOperation((MetadataOperation) operation);
                    }
                    else if (operation instanceof StorageOperation) {
                        //TODO: should we also check that streams still exist in Storage, and that their lengths are what we think they are? Or we leave that to the LogSynchronizer?
                        metadataUpdater.preProcessOperation((StorageOperation) operation);
                        metadataUpdater.acceptOperation((StorageOperation) operation);
                    }
                }
                catch (Exception ex) {
                    // MetadataUpdater can throw StreamSegmentSealedException or MetadataUpdateException.
                    throw new DataCorruptionException(String.format("Unable to update metadata for Log Operation %s", operation), ex);
                }

                // Add to InMemory Operation Log.
                this.memoryLogUpdater.add(operation);
            }
        }

        // Commit whatever changes we have in the metadata updater to the Container Metadata.
        // This code will only be invoked if we haven't encountered any exceptions during recovery.
        metadataUpdater.commit();
        LoggerHelpers.traceLeave(log, this.traceObjectId, "recoverFromDataFrameLog", traceId);
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
        Exceptions.throwIfClosed(this.state == ContainerState.Closed, this);
    }

    private void setState(ContainerState state) {
        if (this.state != state) {
            log.info("{}: StateTransition from {} to {}.", traceObjectId, this.state, state);
        }

        this.state = state;
    }

    //endregion
}
