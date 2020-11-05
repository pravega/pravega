/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.Services;
import io.pravega.common.util.Retry;
import io.pravega.common.util.SequencedItemList;
import io.pravega.segmentstore.contracts.StreamingException;
import io.pravega.segmentstore.server.ContainerOfflineException;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.OperationLog;
import io.pravega.segmentstore.server.ReadIndex;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.segmentstore.server.logs.operations.StorageMetadataCheckpointOperation;
import io.pravega.segmentstore.storage.DataLogCorruptedException;
import io.pravega.segmentstore.storage.DataLogDisabledException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.DurableDataLogFactory;
import io.pravega.segmentstore.storage.LogAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents an OperationLog that durably stores Log Operations it receives.
 */
@Slf4j
@ThreadSafe
public class DurableLog extends AbstractService implements OperationLog {
    //region Members

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private final String traceObjectId;
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
    private final CompletableFuture<Void> delayedStart;
    private final Retry.RetryAndThrowConditionally delayedStartRetry;

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
        this.metadata = Preconditions.checkNotNull(metadata, "metadata");
        Preconditions.checkNotNull(dataFrameLogFactory, "dataFrameLogFactory");
        Preconditions.checkNotNull(readIndex, "readIndex");
        this.executor = Preconditions.checkNotNull(executor, "executor");

        this.durableDataLog = dataFrameLogFactory.createDurableDataLog(metadata.getContainerId());
        assert this.durableDataLog != null : "dataFrameLogFactory created null durableDataLog.";

        this.traceObjectId = String.format("DurableLog[%s]", metadata.getContainerId());
        this.inMemoryOperationLog = createInMemoryLog();
        this.memoryStateUpdater = new MemoryStateUpdater(this.inMemoryOperationLog, readIndex, this::triggerTailReads);
        MetadataCheckpointPolicy checkpointPolicy = new MetadataCheckpointPolicy(config, this::queueMetadataCheckpoint, this.executor);
        this.operationProcessor = new OperationProcessor(this.metadata, this.memoryStateUpdater, this.durableDataLog, checkpointPolicy, executor);
        Services.onStop(this.operationProcessor, this::queueStoppedHandler, this::queueFailedHandler, this.executor);
        this.tailReads = new HashSet<>();
        this.closed = new AtomicBoolean();
        this.delayedStart = new CompletableFuture<>();
        this.delayedStartRetry = Retry.withExpBackoff(config.getStartRetryDelay().toMillis(), 1, Integer.MAX_VALUE)
                                      .retryWhen(ex -> Exceptions.unwrap(ex) instanceof DataLogDisabledException);

    }

    @VisibleForTesting
    protected SequencedItemList<Operation> createInMemoryLog() {
        return new SequencedItemList<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            Futures.await(Services.stopAsync(this, this.executor));

            this.operationProcessor.close();
            this.durableDataLog.close(); // Call this again just in case we were not able to do it in doStop().
            log.info("{}: Closed.", this.traceObjectId);
            this.closed.set(true);
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        log.info("{}: Starting.", this.traceObjectId);
        this.delayedStartRetry
                .runAsync(() -> tryStartOnce()
                        .whenComplete((v, ex) -> {
                            if (ex == null) {
                                // We are done.
                                notifyDelayedStartComplete(null);
                            } else {
                                if (Exceptions.unwrap(ex) instanceof DataLogDisabledException) {
                                    // Place the DurableLog in a Started State, but keep trying to restart.
                                    notifyStartComplete(null);
                                }
                                throw new CompletionException(ex);
                            }
                        }), this.executor)
                .exceptionally(this::notifyDelayedStartComplete);
    }

    private Void notifyDelayedStartComplete(Throwable failureCause) {
        if (failureCause == null) {
            this.delayedStart.complete(null);
        } else {
            this.delayedStart.completeExceptionally(failureCause);
        }

        notifyStartComplete(failureCause);
        return null;
    }

    private void notifyStartComplete(Throwable failureCause) {
        if (failureCause == null && state() == State.STARTING) {
            log.info("{}: Started ({}).", this.traceObjectId, isOffline() ? "OFFLINE" : "Online");
            notifyStarted();
        }

        if (failureCause != null) {
            failureCause = Exceptions.unwrap(failureCause);
            this.stopException.set(failureCause);
            if (state() == State.STARTING) {
                // Make sure we stop the OperationProcessor if we started it, but not before we stop ourselves (with the
                // correct failure cause), otherwise the OperationProcessor's listener will shut us down with a totally
                // different failure cause.
                notifyFailed(failureCause);
                this.operationProcessor.stopAsync();
            } else {
                doStop();
            }
        }
    }

    private CompletableFuture<Void> tryStartOnce() {
        return CompletableFuture
                .supplyAsync(this::performRecovery, this.executor)
                .thenCompose(anyItemsRecovered ->
                        Services.startAsync(this.operationProcessor, this.executor)
                                .thenComposeAsync(v -> anyItemsRecovered ? CompletableFuture.completedFuture(null) : queueMetadataCheckpoint(), this.executor));
    }

    private CompletableFuture<Void> queueMetadataCheckpoint() {
        return Futures.toVoid(checkpoint(DEFAULT_TIMEOUT));
    }

    @SneakyThrows(Exception.class)
    private boolean performRecovery() {
        // Make sure we are in the correct state. We do not want to do recovery while we are in full swing.
        Preconditions.checkState(state() == State.STARTING || (state() == State.RUNNING && isOffline()), "Invalid State for recovery.");

        this.operationProcessor.getMetrics().operationLogInit();
        Timer timer = new Timer();
        try {
            // Initialize the DurableDataLog, which will acquire its lock and ensure we are the only active users of it.
            this.durableDataLog.initialize(DEFAULT_TIMEOUT);

            // Initiate the recovery.
            RecoveryProcessor p = new RecoveryProcessor(this.metadata, this.durableDataLog, this.memoryStateUpdater);
            int recoveredItemCount = p.performRecovery();
            this.operationProcessor.getMetrics().operationsCompleted(recoveredItemCount, timer.getElapsed());

            // Verify that the Recovery Processor has left the metadata in a non-recovery mode.
            Preconditions.checkState(!this.metadata.isRecoveryMode(), "Recovery completed but Metadata is still in Recovery Mode.");
            return recoveredItemCount > 0;
        } catch (Exception ex) {
            log.error("{} Recovery FAILED.", this.traceObjectId, ex);
            Throwable cause = Exceptions.unwrap(ex);
            if (cause instanceof DataCorruptionException || cause instanceof DataLogCorruptedException) {
                // DataCorruptionException during recovery means we will be unable to execute the recovery successfully
                // regardless how many times we try. We need to disable the log so that future instances of this class
                // will not attempt to do so indefinitely (which could wipe away useful debugging information before
                // someone can manually fix the problem).
                try {
                    this.durableDataLog.disable();
                    log.info("{} Log disabled due to {} during recovery.", this.traceObjectId, cause.getClass().getSimpleName());
                } catch (Exception disableEx) {
                    log.warn("{}: Unable to disable log after DataCorruptionException during recovery.", this.traceObjectId, disableEx);
                    ex.addSuppressed(disableEx);
                }
            }

            throw ex;
        }
    }

    @Override
    protected void doStop() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, traceObjectId, "doStop");
        log.info("{}: Stopping.", this.traceObjectId);
        Services.stopAsync(this.operationProcessor, this.executor)
                .whenCompleteAsync((r, ex) -> {
                    cancelTailReads();

                    this.durableDataLog.close();
                    Throwable cause = this.stopException.get();
                    if (cause == null && this.operationProcessor.state() == State.FAILED) {
                        cause = this.operationProcessor.failureCause();
                    }

                    // Terminate the delayed start future now, if still active.
                    this.delayedStart.completeExceptionally(cause == null ? new ObjectClosedException(this) : cause);

                    if (cause == null) {
                        // Normal shutdown.
                        notifyStopped();
                    } else {
                        // Shutdown caused by some failure.
                        notifyFailed(cause);
                    }

                    log.info("{}: Stopped.", this.traceObjectId);
                    LoggerHelpers.traceLeave(log, traceObjectId, "doStop", traceId);
                }, this.executor)
                .exceptionally(ex -> {
                    notifyFailed(ex);
                    return null;
                });
    }

    //endregion

    //region Container Implementation

    @Override
    public int getId() {
        return this.metadata.getContainerId();
    }

    @Override
    public boolean isOffline() {
        return !this.delayedStart.isDone();
    }

    //endregion

    //region OperationLog Implementation

    @Override
    public CompletableFuture<Void> add(Operation operation, OperationPriority priority, Duration timeout) {
        ensureRunning();
        return this.operationProcessor.process(operation, priority);
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

        // Before we do any real truncation, we need to mini-snapshot the metadata with only those fields that are updated
        // asynchronously for us (i.e., not via normal Log Operations) such as the Storage State. That ensures that this
        // info will be readily available upon recovery without delay.
        return add(new StorageMetadataCheckpointOperation(), OperationPriority.High, timer.getRemaining())
                .thenComposeAsync(v -> this.durableDataLog.truncate(truncationFrameAddress, timer.getRemaining()), this.executor)
                .thenRunAsync(() -> {
                    // Truncate InMemory Transaction Log.
                    int count = this.inMemoryOperationLog.truncate(actualTruncationSequenceNumber);

                    // Remove old truncation markers.
                    this.metadata.removeTruncationMarkers(actualTruncationSequenceNumber);
                    this.operationProcessor.getMetrics().operationLogTruncate(count);
                }, this.executor);
    }

    @Override
    public CompletableFuture<Long> checkpoint(Duration timeout) {
        log.debug("{}: Queuing MetadataCheckpointOperation.", this.traceObjectId);
        MetadataCheckpointOperation op = new MetadataCheckpointOperation();
        return this.operationProcessor
                .process(op, OperationPriority.Normal)
                .thenApply(v -> {
                    log.info("{}: MetadataCheckpointOperation durably stored.", this.traceObjectId);
                    return op.getSequenceNumber();
                });
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
    public CompletableFuture<Void> awaitOnline() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        if (state() != State.RUNNING) {
            throw new IllegalContainerStateException(this.getId(), state(), State.RUNNING);
        }

        return this.delayedStart;
    }

    //endregion

    //region Helpers

    private void ensureRunning() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        if (state() != State.RUNNING) {
            throw new IllegalContainerStateException(getId(), state(), State.RUNNING);
        } else if (isOffline()) {
            throw new ContainerOfflineException(getId());
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
            log.warn("{}: OperationProcessor stopped unexpectedly (no error) but DurableLog was not currently stopping. Shutting down DurableLog.", this.traceObjectId);
            this.stopException.set(new StreamingException("OperationProcessor stopped unexpectedly (no error) but DurableLog was not currently stopping."));
            stopAsync();
        }
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
                tr.future.complete(Futures.runOrFail(
                        () -> this.inMemoryOperationLog.read(tr.afterSequenceNumber, tr.maxCount),
                        tr.future));
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
            this.future = Futures.futureWithTimeout(timeout, executor);
        }

        @Override
        public String toString() {
            return String.format("SeqNo = %d, Count = %d", this.afterSequenceNumber, this.maxCount);
        }
    }

    //endregion
}
