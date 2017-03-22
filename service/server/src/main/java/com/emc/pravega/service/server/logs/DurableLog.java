/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.logs;

import com.emc.pravega.common.ExceptionHelpers;
import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.concurrent.ServiceShutdownListener;
import com.emc.pravega.common.util.SequencedItemList;
import com.emc.pravega.service.contracts.ContainerException;
import com.emc.pravega.service.contracts.StreamSegmentException;
import com.emc.pravega.service.contracts.StreamingException;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.IllegalContainerStateException;
import com.emc.pravega.service.server.LogItemFactory;
import com.emc.pravega.service.server.OperationLog;
import com.emc.pravega.service.server.ReadIndex;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.logs.operations.MetadataCheckpointOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.OperationFactory;
import com.emc.pravega.service.server.logs.operations.ProbeOperation;
import com.emc.pravega.service.storage.DurableDataLog;
import com.emc.pravega.service.storage.DurableDataLogFactory;
import com.emc.pravega.service.storage.LogAddress;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents an OperationLog that durably stores Log Operations it receives.
 */
@Slf4j
@ThreadSafe
public class DurableLog extends AbstractService implements OperationLog {
    //region Members

    private static final Duration RECOVERY_TIMEOUT = Duration.ofSeconds(30);
    private final String traceObjectId;
    private final DurableLogConfig config;
    private final LogItemFactory<Operation> operationFactory;
    private final SequencedItemList<Operation> inMemoryOperationLog;
    private final DurableDataLog durableDataLog;
    private final MemoryStateUpdater memoryStateUpdater;
    private final OperationProcessor operationProcessor;
    private final UpdateableContainerMetadata metadata;
    @GuardedBy("tailReads")
    private final Set<TailRead> tailReads;
    private final ScheduledExecutorService executor;
    private final AtomicReference<Throwable> stopException = new AtomicReference<>();
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DurableLog class.
     *
     * @param config              Durable Log Configuration.
     * @param metadata            The StreamSegment Container Metadata for the container which this Durable Log is part of.
     * @param dataFrameLogFactory A DurableDataLogFactory which can be used to create instances of DataFrameLogs.
     * @param readIndex           A ReadIndex which can be used to store newly processed appends.
     * @param executor            The Executor to use for async operations.
     * @throws NullPointerException If any of the arguments are null.
     */
    public DurableLog(DurableLogConfig config, UpdateableContainerMetadata metadata, DurableDataLogFactory dataFrameLogFactory, ReadIndex readIndex, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(metadata, "metadata");
        Preconditions.checkNotNull(dataFrameLogFactory, "dataFrameLogFactory");
        Preconditions.checkNotNull(readIndex, "readIndex");
        Preconditions.checkNotNull(executor, "executor");

        this.config = config;
        this.durableDataLog = dataFrameLogFactory.createDurableDataLog(metadata.getContainerId());
        assert this.durableDataLog != null : "dataFrameLogFactory created null durableDataLog.";

        this.traceObjectId = String.format("DurableLog[%s]", metadata.getContainerId());
        this.metadata = metadata;
        this.executor = executor;
        this.operationFactory = new OperationFactory();
        this.inMemoryOperationLog = new SequencedItemList<>();
        this.memoryStateUpdater = new MemoryStateUpdater(this.inMemoryOperationLog, readIndex, this::triggerTailReads);
        MetadataCheckpointPolicy checkpointPolicy = new MetadataCheckpointPolicy(this.config, this::queueMetadataCheckpoint, this.executor);
        this.operationProcessor = new OperationProcessor(this.metadata, this.memoryStateUpdater, this.durableDataLog, checkpointPolicy, executor);
        this.operationProcessor.addListener(new ServiceShutdownListener(this::queueStoppedHandler, this::queueFailedHandler), this.executor);
        this.tailReads = new HashSet<>();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            stopAsync();
            ServiceShutdownListener.awaitShutdown(this, false);

            this.operationProcessor.close();
            this.durableDataLog.close();
            log.info("{}: Closed.", this.traceObjectId);
            this.closed.set(true);
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, traceObjectId, "doStart");
        log.info("{}: Starting.", this.traceObjectId);

        this.executor.execute(() -> {
            try {
                boolean anyItemsRecovered = performRecovery();
                this.operationProcessor.startAsync().awaitRunning();
                if (!anyItemsRecovered) {
                    // If the DurableLog is empty, need to queue a MetadataCheckpointOperation so we have a valid starting state (and wait for it).
                    queueMetadataCheckpoint().get(RECOVERY_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                }
            } catch (Exception ex) {
                if (this.operationProcessor.isRunning()) {
                    // Make sure we stop the operation processor if we started it.
                    this.operationProcessor.stopAsync();
                }

                notifyFailed(ExceptionHelpers.getRealException(ex));
                return;
            }

            // If we got here, all is good. We were able to start successfully.
            log.info("{}: Started.", this.traceObjectId);
            notifyStarted();
            LoggerHelpers.traceLeave(log, traceObjectId, "doStart", traceId);
        });
    }

    @Override
    protected void doStop() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, traceObjectId, "doStop");
        log.info("{}: Stopping.", this.traceObjectId);
        this.operationProcessor.stopAsync();

        this.executor.execute(() -> {
            ServiceShutdownListener.awaitShutdown(this.operationProcessor, false);

            cancelTailReads();

            Throwable cause = this.stopException.get();
            if (cause == null && this.operationProcessor.state() == State.FAILED) {
                cause = this.operationProcessor.failureCause();
            }

            if (cause == null) {
                // Normal shutdown.
                notifyStopped();
            } else {
                // Shutdown caused by some failure.
                notifyFailed(cause);
            }

            log.info("{}: Stopped.", this.traceObjectId);
            LoggerHelpers.traceLeave(log, traceObjectId, "doStop", traceId);
        });
    }

    //endregion

    //region Container Implementation

    @Override
    public int getId() {
        return this.metadata.getContainerId();
    }

    //endregion

    //region OperationLog Implementation

    @Override
    public CompletableFuture<Long> add(Operation operation, Duration timeout) {
        ensureRunning();
        return this.operationProcessor.process(operation);
    }

    @Override
    public CompletableFuture<Void> truncate(long upToSequenceNumber, Duration timeout) {
        ensureRunning();
        Preconditions.checkArgument(this.metadata.isValidTruncationPoint(upToSequenceNumber), "Invalid Truncation Point. Must refer to a MetadataCheckpointOperation.");

        // The SequenceNumber we were given points directly to a MetadataCheckpointOperation. We must not remove it!
        // Instead, it must be the first operation that does survive, so we need to adjust our SeqNo to the one just
        // before it.
        long actualTruncationSequenceNumber = upToSequenceNumber - 1;

        // Find the closest Truncation Marker (that does not exceed it).
        LogAddress truncationFrameAddress = this.metadata.getClosestTruncationMarker(actualTruncationSequenceNumber);
        if (truncationFrameAddress == null) {
            // Nothing to truncate.
            return CompletableFuture.completedFuture(null);
        }

        TimeoutTimer timer = new TimeoutTimer(timeout);
        log.info("{}: Truncate (OperationSequenceNumber = {}, DataFrameAddress = {}).", this.traceObjectId, upToSequenceNumber, truncationFrameAddress);

        return this.durableDataLog
                .truncate(truncationFrameAddress, timer.getRemaining())
                .thenRunAsync(() -> {
                    // Truncate InMemory Transaction Log.
                    this.inMemoryOperationLog.truncate(actualTruncationSequenceNumber);

                    // Remove old truncation markers.
                    this.metadata.removeTruncationMarkers(actualTruncationSequenceNumber);
                }, this.executor);
    }

    @Override
    public CompletableFuture<Iterator<Operation>> read(long afterSequenceNumber, int maxCount, Duration timeout) {
        ensureRunning();
        log.debug("{}: Read (AfterSequenceNumber = {}, MaxCount = {}).", this.traceObjectId, afterSequenceNumber, maxCount);
        Iterator<Operation> logReadResult = this.inMemoryOperationLog.read(afterSequenceNumber, maxCount);
        if (logReadResult.hasNext()) {
            // Data is readily available.
            return CompletableFuture.completedFuture(logReadResult);
        } else {
            // Register a tail read and return the future for it.
            CompletableFuture<Iterator<Operation>> result = null;
            Operation lastOp;
            synchronized (this.tailReads) {
                lastOp = this.inMemoryOperationLog.getLast();
                if (lastOp == null || lastOp.getSequenceNumber() <= afterSequenceNumber) {
                    // We cannot fulfill this at this moment; let it be triggered when we do get a new operation.
                    TailRead tailRead = new TailRead(afterSequenceNumber, maxCount, timeout, this.executor);
                    result = tailRead.future;
                    this.tailReads.add(tailRead);
                    result.whenComplete((r, ex) -> unregisterTailRead(tailRead));
                }
            }

            if (result == null) {
                // If we get here, it means that we have since received an operation (after the original call, but before
                // entering the synchronized block above); re-issue the read and return the result.
                logReadResult = this.inMemoryOperationLog.read(afterSequenceNumber, maxCount);
                assert logReadResult.hasNext() :
                        String.format("Unable to read anything after SeqNo %d, even though last operation SeqNo == %d",
                                afterSequenceNumber, lastOp == null ? -1 : lastOp.getSequenceNumber());
                result = CompletableFuture.completedFuture(logReadResult);
            }

            return result;
        }
    }

    @Override
    public CompletableFuture<Void> operationProcessingBarrier(Duration timeout) {
        return FutureHelpers
                .toVoid(add(new ProbeOperation(), timeout))
                .whenComplete((r, ex) -> {
                    // We don't care if this operation completed successfully or not. The Operation Barrier needs to complete
                    // when all operations prior to it completed, regardless of outcome.
                    if (ex != null) {
                        log.warn("{}: Error caught while waiting for {}: {}.", this.traceObjectId, ProbeOperation.class.getSimpleName(), ex);
                    }
                });
    }

    //endregion

    //region Recovery

    private boolean performRecovery() throws Exception {
        // Make sure we are in the correct state. We do not want to do recovery while we are in full swing.
        Preconditions.checkState(state() == State.STARTING, "Cannot perform recovery if the DurableLog is not in a '%s' state.", State.STARTING);

        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "performRecovery");
        TimeoutTimer timer = new TimeoutTimer(RECOVERY_TIMEOUT);
        log.info("{} Recovery started.", this.traceObjectId);

        // Put metadata (and entire container) into 'Recovery Mode'.
        this.metadata.enterRecoveryMode();

        // Reset metadata.
        this.metadata.reset();

        OperationMetadataUpdater metadataUpdater = new OperationMetadataUpdater(this.metadata);
        this.memoryStateUpdater.enterRecoveryMode(metadataUpdater);

        boolean successfulRecovery = false;
        boolean anyItemsRecovered;
        try {
            this.durableDataLog.initialize(timer.getRemaining());
            anyItemsRecovered = recoverFromDataFrameLog(metadataUpdater);
            log.info("{} Recovery completed. Items Recovered = {}.", this.traceObjectId, anyItemsRecovered);
            successfulRecovery = true;
        } catch (Exception ex) {
            log.error("{} Recovery FAILED. {}", this.traceObjectId, ex);
            throw ex;
        } finally {
            // We must exit recovery mode when done, regardless of outcome.
            this.metadata.exitRecoveryMode();
            this.memoryStateUpdater.exitRecoveryMode(successfulRecovery);
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "performRecovery", traceId);
        return anyItemsRecovered;
    }

    /**
     * Recovers the Operations from the DurableLog using the given OperationMetadataUpdater. Searches the DurableDataLog
     * until the first MetadataCheckpointOperation is encountered. All Operations prior to this one are skipped over.
     * Recovery starts with the first MetadataCheckpointOperation and runs until the end of the DurableDataLog is reached.
     * Subsequent MetadataCheckpointOperations are ignored (as they contain redundant information - which has already
     * been built up using the Operations up to them).
     *
     * @param metadataUpdater The OperationMetadataUpdater to use for updates.
     * @return True if any operations were recovered, false otherwise.
     */
    private boolean recoverFromDataFrameLog(OperationMetadataUpdater metadataUpdater) throws Exception {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "recoverFromDataFrameLog");
        int skippedOperationCount = 0;
        int skippedDataFramesCount = 0;
        int recoveredItemCount = 0;

        // Read all entries from the DataFrameLog and append them to the InMemoryOperationLog.
        // Also update metadata along the way.
        try (DataFrameReader<Operation> reader = new DataFrameReader<>(this.durableDataLog, this.operationFactory, getId())) {
            DataFrameReader.ReadResult<Operation> readResult;

            // We can only recover starting from a MetadataCheckpointOperation; find the first one.
            while (true) {
                // Fetch the next operation.
                readResult = reader.getNext();
                if (readResult == null) {
                    // We have reached the end and have not found any MetadataCheckpointOperations.
                    log.warn("{}: Reached the end of the DataFrameLog and could not find any MetadataCheckpointOperations after reading {} Operations and {} Data Frames.", this.traceObjectId, skippedOperationCount, skippedDataFramesCount);
                    break;
                } else if (readResult.getItem() instanceof MetadataCheckpointOperation) {
                    // We found a checkpoint. Start recovering from here.
                    log.info("{}: Starting recovery from Sequence Number {} (skipped {} Operations and {} Data Frames).", this.traceObjectId, readResult.getItem().getSequenceNumber(), skippedOperationCount, skippedDataFramesCount);
                    break;
                } else if (readResult.isLastFrameEntry()) {
                    skippedDataFramesCount++;
                }

                skippedOperationCount++;
                log.debug("{}: Not recovering operation because no MetadataCheckpointOperation encountered so far ({}).", this.traceObjectId, readResult.getItem());
            }

            // Now continue with the recovery from here.
            while (readResult != null) {
                recordTruncationMarker(readResult, metadataUpdater);
                recoverOperation(readResult.getItem(), metadataUpdater);
                recoveredItemCount++;

                // Fetch the next operation.
                readResult = reader.getNext();
            }
        }

        // Commit whatever changes we have in the metadata updater to the Container Metadata.
        // This code will only be invoked if we haven't encountered any exceptions during recovery.
        metadataUpdater.commit();
        LoggerHelpers.traceLeave(log, this.traceObjectId, "recoverFromDataFrameLog", traceId, recoveredItemCount);
        return recoveredItemCount > 0;
    }

    private void recoverOperation(Operation operation, OperationMetadataUpdater metadataUpdater) throws DataCorruptionException {
        // Update Metadata Sequence Number.
        metadataUpdater.setOperationSequenceNumber(operation.getSequenceNumber());

        // Update the metadata with the information from the Operation.
        try {
            log.debug("{} Recovering {}.", this.traceObjectId, operation);
            metadataUpdater.preProcessOperation(operation);
            metadataUpdater.acceptOperation(operation);
        } catch (StreamSegmentException | ContainerException ex) {
            // Metadata updates failures should not happen during recovery.
            throw new DataCorruptionException(String.format("Unable to update metadata for Log Operation %s", operation), ex);
        }

        // Update in-memory structures.
        this.memoryStateUpdater.process(operation);
    }

    private void recordTruncationMarker(DataFrameReader.ReadResult<Operation> readResult, OperationMetadataUpdater metadataUpdater) {
        // Determine and record Truncation Markers, but only if the current operation spans multiple DataFrames
        // or it's the last entry in a DataFrame.
        LogAddress lastFullAddress = readResult.getLastFullDataFrameAddress();
        LogAddress lastUsedAddress = readResult.getLastUsedDataFrameAddress();
        if (lastFullAddress != null && lastFullAddress.getSequence() != lastUsedAddress.getSequence()) {
            // This operation spans multiple DataFrames. The TruncationMarker should be set on the last DataFrame
            // that ends with a part of it.
            metadataUpdater.recordTruncationMarker(readResult.getItem().getSequenceNumber(), lastFullAddress);
        } else if (readResult.isLastFrameEntry()) {
            // The operation was the last one in the frame. This is a Truncation Marker.
            metadataUpdater.recordTruncationMarker(readResult.getItem().getSequenceNumber(), lastUsedAddress);
        }
    }

    //endregion

    //region Helpers

    private void ensureRunning() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        if (state() != State.RUNNING) {
            throw new IllegalContainerStateException(this.getId(), state(), State.RUNNING);
        }
    }

    private void queueFailedHandler(Throwable cause) {
        // The Queue Processor failed. We need to shut down right away.
        log.warn("{}: QueueProcessor failed with exception {}", this.traceObjectId, cause);
        this.stopException.set(cause);
        stopAsync();
    }

    private void queueStoppedHandler() {
        if (state() != State.STOPPING && state() != State.FAILED) {
            // The Queue Processor stopped but we are not in a stopping phase. We need to shut down right away.
            log.warn("{}: QueueProcessor stopped unexpectedly (no error) but DurableLog was not currently stopping. Shutting down DurableLog.", this.traceObjectId);
            this.stopException.set(new StreamingException("QueueProcessor stopped unexpectedly (no error) but DurableLog was not currently stopping."));
            stopAsync();
        }
    }

    private CompletableFuture<Void> queueMetadataCheckpoint() {
        log.info("{}: MetadataCheckpointOperation queued.", this.traceObjectId);
        return this.operationProcessor
                .process(new MetadataCheckpointOperation())
                .thenAccept(seqNo -> log.info("{}: MetadataCheckpointOperation durably stored.", this.traceObjectId));
    }

    private void unregisterTailRead(TailRead tailRead) {
        synchronized (this.tailReads) {
            this.tailReads.remove(tailRead);
        }

        if (tailRead.future != null && !tailRead.future.isDone()) {
            tailRead.future.cancel(true);
        }
    }

    private void triggerTailReads() {
        this.executor.execute(() -> {
            // Gather all the eligible tail reads.
            List<TailRead> toTrigger;
            synchronized (this.tailReads) {
                Operation lastOp = this.inMemoryOperationLog.getLast();
                if (lastOp != null) {
                    long seqNo = lastOp.getSequenceNumber();
                    toTrigger = this.tailReads.stream().filter(e -> e.afterSequenceNumber < seqNo).collect(Collectors.toList());
                } else {
                    toTrigger = Collections.emptyList();
                }
            }

            // Trigger all of them (no need to unregister them; the unregister handle is already wired up).
            for (TailRead tr : toTrigger) {
                tr.future.complete(FutureHelpers.runOrFail(() -> {
                    return this.inMemoryOperationLog.read(tr.afterSequenceNumber, tr.maxCount);
                }, tr.future));
            }
        });
    }

    private void cancelTailReads() {
        List<TailRead> reads;
        synchronized (this.tailReads) {
            reads = new ArrayList<>(this.tailReads);
        }

        reads.forEach(this::unregisterTailRead);
    }

    //endregion

    //region TailRead

    /**
     * Holds information about pending Tail Reads.
     */
    private static class TailRead {
        final long afterSequenceNumber;
        final int maxCount;
        final CompletableFuture<Iterator<Operation>> future;

        TailRead(long afterSequenceNumber, int maxCount, Duration timeout, ScheduledExecutorService executor) {
            this.afterSequenceNumber = afterSequenceNumber;
            this.maxCount = maxCount;
            this.future = FutureHelpers.futureWithTimeout(timeout, executor);
        }

        @Override
        public String toString() {
            return String.format("SeqNo = %d, Count = %d", this.afterSequenceNumber, this.maxCount);
        }
    }

    //endregion
}
