package com.emc.logservice.server.logs;

import com.emc.logservice.server.*;
import com.emc.logservice.server.containers.TruncationMarkerCollection;
import com.emc.logservice.common.*;
import com.emc.logservice.storageabstraction.*;
import com.emc.logservice.server.logs.operations.*;
import com.emc.logservice.storageabstraction.DurableDataLog;
import com.emc.logservice.storageabstraction.DurableDataLogFactory;

import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

/**
 * Represents an OperationLog that durably stores Log Operations it receives.
 */
public class DurableLog implements OperationLog {
    //region Members

    private static final Duration CloseTimeout = Duration.ofSeconds(30); // TODO: make configurable.
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
    private boolean closed;

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
        if (metadata == null) {
            throw new NullPointerException("metadata");
        }

        if (dataFrameLogFactory == null) {
            throw new NullPointerException("dataFrameLogFactory");
        }

        if (cache == null) {
            throw new NullPointerException("cache");
        }

        this.dataFrameLog = dataFrameLogFactory.createDurableDataLog(metadata.getContainerId());
        if (this.dataFrameLog == null) {
            throw new AssertionError("DurableDataLogFactory created a null DataFrameLog");
        }

        this.metadata = metadata;
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
    public void close() throws Exception {
        if (!this.closed) {
            if (this.state == ContainerState.Started) {
                // Stop the container if it's currently running.
                this.stop(CloseTimeout).get();
            }

            this.queueProcessor.close();
            this.dataFrameLog.close();
            this.closed = true;
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
            ContainerState.Initialized.checkValidPreviousState(this.state);

            // Perform Recovery and initialize all components.
            return performRecovery(timer.getRemaining()) // Perform Recovery.
                                                         .thenRun(() -> this.queueProcessor.initialize(timer.getRemaining())) // Initialize Queue Processor.
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
            return this.queueProcessor.start(timeout)
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

            // Stop the Operation Queue Processor.
            return this.queueProcessor.stop(timeout);
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
        return this.dataFrameLog.truncate(dataFrameSeqNo, timer.getRemaining()) // Truncate DataFrameLog.
                                .thenApply(r -> this.inMemoryOperationLog.truncate(e -> e.getSequenceNumber() <= upToSequenceNumber)) // Truncate InMemory Transaction Log.
                                .thenRun(() -> this.truncationMarkers.removeTruncationMarkers(upToSequenceNumber)); // Remove old truncation markers.
    }

    @Override
    public CompletableFuture<Iterator<Operation>> read(long afterSequenceNumber, int maxCount, Duration timeout) {
        ensureStarted();
        return CompletableFuture.completedFuture(this.inMemoryOperationLog.read(e -> e.getSequenceNumber() > afterSequenceNumber, maxCount));
    }

    //endregion

    //region Recovery

    private CompletableFuture<Void> performRecovery(Duration timeout) {
        // Make sure we are in the correct state. We do not want to do recovery while we are in full swing.
        ContainerState.Initialized.checkValidPreviousState(this.state);

        TimeoutTimer timer = new TimeoutTimer(timeout);

        // Put metadata (and entire container) into 'Recovery Mode'.
        this.metadata.enterRecoveryMode();
        this.truncationMarkers.enterRecoveryMode();

        // Reset metadata.
        this.metadata.reset();
        this.truncationMarkers.reset();

        OperationMetadataUpdater metadataUpdater = new OperationMetadataUpdater(this.metadata, this.truncationMarkers);
        this.memoryLogUpdater.enterRecoveryMode(metadataUpdater);

        CompletableFuture<Void> result = this.dataFrameLog.initialize(timer.getRemaining()) // Iniialize DataFrameLog.
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
        });
        return result;
    }

    private void recoverFromDataFrameLog(OperationMetadataUpdater metadataUpdater, Duration timeout) throws DataCorruptionException, DurableDataLogException {
        TimeoutTimer timer = new TimeoutTimer(timeout);

        // Read all entries from the DataFrameLog and append them to the InMemoryOperationLog.
        // Also update metadata along the way.
        try (DataFrameReader reader = new DataFrameReader(this.dataFrameLog)) {
            DataFrameReader.ReadResult lastReadResult = null;
            while (true) {
                // Fetch the next operation.
                DataFrameReader.ReadResult readResult = reader.getNextOperation(timer.getRemaining()).join();
                if (readResult == null) {
                    // We have reached the end.
                    break;
                }

                Operation operation = readResult.getOperation();

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
                    System.out.println(String.format("Recovering %s", operation));
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
