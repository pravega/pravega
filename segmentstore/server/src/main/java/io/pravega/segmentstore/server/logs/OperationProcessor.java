/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.base.Preconditions;
import io.pravega.common.ExceptionHelpers;
import io.pravega.common.MathHelpers;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.AbstractThreadPoolService;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.function.CallbackHelpers;
import io.pravega.common.util.BlockingDrainingQueue;
import io.pravega.common.util.SortedDeque;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.CompletableOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.QueueStats;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Lombok;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Single-thread Processor for Operations. Queues all incoming entries in a BlockingDrainingQueue, then picks them all
 * at once, generates DataFrames from them and commits them to the DataFrameLog, one by one, in sequence.
 */
@Slf4j
class OperationProcessor extends AbstractThreadPoolService implements AutoCloseable {
    //region Members

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private static final int MAX_READ_AT_ONCE = 1000;

    private final UpdateableContainerMetadata metadata;
    @GuardedBy("stateLock")
    private final OperationMetadataUpdater metadataUpdater;
    private final DurableDataLog durableDataLog;
    private final BlockingDrainingQueue<CompletableOperation> operationQueue;
    private final Object stateLock = new Object();
    private final QueueProcessingState state;
    @GuardedBy("stateLock")
    private final DataFrameBuilder<Operation> dataFrameBuilder;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the OperationProcessor class.
     *
     * @param metadata         The ContainerMetadata for the Container to process operations for.
     * @param stateUpdater     A MemoryStateUpdater that is used to update in-memory structures upon successful Operation committal.
     * @param durableDataLog   The DataFrameLog to write DataFrames to.
     * @param checkpointPolicy The Checkpoint Policy for Metadata.
     * @param executor         An Executor to use for async operations.
     * @throws NullPointerException If any of the arguments are null.
     */
    OperationProcessor(UpdateableContainerMetadata metadata, MemoryStateUpdater stateUpdater, DurableDataLog durableDataLog, MetadataCheckpointPolicy checkpointPolicy, ScheduledExecutorService executor) {
        super(String.format("OperationProcessor[%d]", metadata.getContainerId()), executor);
        this.metadata = metadata;
        this.metadataUpdater = new OperationMetadataUpdater(this.metadata);
        this.durableDataLog = Preconditions.checkNotNull(durableDataLog, "durableDataLog");
        this.operationQueue = new BlockingDrainingQueue<>();
        this.state = new QueueProcessingState(stateUpdater, checkpointPolicy);
        val args = new DataFrameBuilder.Args(this.state::frameSealed, this.state::commit, this.state::fail, this.executor);
        this.dataFrameBuilder = new DataFrameBuilder<>(this.durableDataLog, args);
    }

    //endregion

    //region AbstractThreadPoolService Implementation

    @Override
    protected Duration getShutdownTimeout() {
        return SHUTDOWN_TIMEOUT;
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        return FutureHelpers
                .loop(this::isRunning,
                        () -> delayIfNecessary()
                                .thenComposeAsync(v -> this.operationQueue.take(MAX_READ_AT_ONCE), this.executor)
                                .thenAcceptAsync(this::processOperations, this.executor),
                        this.executor)
                .exceptionally(this::iterationErrorHandler);
    }

    @Override
    protected void doStop() {
        // We need to first stop the queue, which will prevent any new items from being processed.
        Throwable ex = new CancellationException("OperationProcessor is shutting down.");
        closeQueue(ex);

        // Close the DataFrameBuilder and cancel any operations caught in limbo.
        synchronized (this.stateLock) {
            this.dataFrameBuilder.close();
        }

        this.state.fail(ex, null);
        super.doStop();
    }

    @Override
    protected void errorHandler(Throwable ex) {
        ex = ExceptionHelpers.getRealException(ex);
        closeQueue(ex);
        if (!isShutdownException(ex)) {
            // Shutdown exceptions means we are already stopping, so no need to do anything else. For all other cases,
            // record the failure and then stop the OperationProcessor.
            super.errorHandler(ex);
            stopAsync();
        }
    }

    @SneakyThrows
    private Void iterationErrorHandler(Throwable ex) {
        ex = ExceptionHelpers.getRealException(ex);
        // If we get an ObjectClosedException while we are shutting down, then it's safe to ignore it. It was most likely
        // caused by the queue being shut down, but the main processing loop has just started another iteration and they
        // crossed paths.
        State s = state();
        boolean isExpected = isShutdownException(ex) && (s == State.STOPPING || s == State.TERMINATED || s == State.FAILED);
        if (!isExpected) {
            throw ex;
        }

        return null;
    }

    private boolean isShutdownException(Throwable ex) {
        return ex instanceof ObjectClosedException || ex instanceof CancellationException;
    }

    //endregion

    //region Operations

    /**
     * Processes the given Operation. This method returns when the given Operation has been added to the internal queue.
     *
     * @param operation The Operation to process.
     * @return A CompletableFuture that, when completed, will indicate the Operation has finished processing. If the
     * Operation completed successfully, the Future will contain the Sequence Number of the Operation. If the Operation
     * failed, it will contain the exception that caused the failure.
     * @throws IllegalContainerStateException If the OperationProcessor is not running.
     */
    public CompletableFuture<Long> process(Operation operation) {
        CompletableFuture<Long> result = new CompletableFuture<>();
        if (!isRunning()) {
            result.completeExceptionally(new IllegalContainerStateException("OperationProcessor is not running."));
        } else {
            log.debug("{}: process {}.", this.traceObjectId, operation);
            try {
                this.operationQueue.add(new CompletableOperation(operation, result));
            } catch (Throwable e) {
                if (ExceptionHelpers.mustRethrow(e)) {
                    throw e;
                }

                result.completeExceptionally(e);
            }
        }

        return result;
    }

    //endregion

    //region Queue Processing

    private CompletableFuture<Void> delayIfNecessary() {
        QueueStats stats = this.durableDataLog.getQueueStatistics();

        // The higher the average fill rate, the more efficient use we make of the available capacity. As such, for high
        // fill rates we don't want to wait too long.
        double fillRateAdj = MathHelpers.minMax(1 - stats.getAverageItemFillRate(), 0, 1);

        // If the queue is below (or very close to) the max degree of parallelism, do our best to fill it up. Otherwise
        // the Fill Rate and the ExpectedProcessingTime will account for queue size as well.
        double countRateAdj = stats.getSize() < stats.getMaxParallelism() ? 0.1 : 1;

        // Finally, we use the the ExpectedProcessingTime to give us a baseline as to how long items usually take to process.
        int delayMillis = (int) (stats.getExpectedProcessingTimeMillis() * fillRateAdj * countRateAdj);
        delayMillis = Math.min(delayMillis, 1000);
        return FutureHelpers.delayedFuture(Duration.ofMillis(delayMillis), this.executor);
    }

    /**
     * Processes a set of pending operations (essentially a single iteration of the QueueProcessor).
     * Steps:
     * <ol>
     * <li> Picks the next items from the queue
     * <li> Creates a DataFrameBuilder and starts appending items to it.
     * <li> As the DataFrameBuilder acknowledges DataFrames being published, acknowledge the corresponding Operations as well.
     * <li> If at the end, the Queue still has items to process, processes those as well.
     * </ol>
     *
     * @param operations The initial set of operations to process (in order). Multiple operations may be processed eventually
     *                   depending on how the operationQueue changes while this is processing.
     */
    private void processOperations(Queue<CompletableOperation> operations) {
        log.debug("{}: processOperations (OperationCount = {}).", this.traceObjectId, operations.size());

        // Process the operations in the queue. This loop will ensure we do continuous processing in case new items
        // arrived while we were busy handling the current items.
        while (!operations.isEmpty()) {
            try {
                // Process the current set of operations.
                while (!operations.isEmpty()) {
                    CompletableOperation o = operations.poll();
                    try {
                        processOperation(o);
                        this.state.addPending(o);
                    } catch (Throwable ex) {
                        ex = ExceptionHelpers.getRealException(ex);
                        this.state.failOperation(o, ex);
                        if (isFatalException(ex)) {
                            // If we encountered an unrecoverable error then we cannot proceed - rethrow the Exception
                            // and let it be handled by the enclosing try-catch. Otherwise, we only need to fail this
                            // operation as its failure is isolated to itself (most likely it's invalid).
                            throw ex;
                        }
                    }
                }

                // Check if there are more operations to process. If so, it's more efficient to process them now (no thread
                // context switching, better DataFrame occupancy optimization) rather than by going back to run().
                if (operations.isEmpty()) {
                    operations = this.operationQueue.poll(MAX_READ_AT_ONCE);
                    if (operations.isEmpty()) {
                        log.debug("{}: processOperations (Flush).", this.traceObjectId);
                        synchronized (this.stateLock) {
                            this.dataFrameBuilder.flush();
                        }
                    } else {
                        log.debug("{}: processOperations (Add OperationCount = {}).", this.traceObjectId, operations.size());
                    }
                }
            } catch (Throwable ex) {
                // Fail ALL the operations that haven't been acknowledged yet.
                ex = ExceptionHelpers.getRealException(ex);
                this.state.fail(ex, null);

                if (isFatalException(ex)) {
                    // If we encountered a fatal exception, it means we detected something that we cannot possibly recover from.
                    // We need to shutdown right away (this will be done by the main loop).

                    // But first, fail any Operations that we did not have a chance to process yet.
                    cancelIncompleteOperations(operations, ex);
                    throw Lombok.sneakyThrow(ex);
                }
            }
        }
    }

    /**
     * Processes a single operation.
     * Steps:
     * <ol>
     * <li> Pre-processes operation (in MetadataUpdater).
     * <li> Assigns Sequence Number.
     * <li> Appends to DataFrameBuilder.
     * <li> Accepts operation in MetadataUpdater.
     * </ol>
     *
     * @param operation        The operation to process.
     * @throws Exception If an exception occurred while processing this operation. Depending on the type of the exception,
     * this could be due to the operation itself being invalid, or because we are unable to process any more operations.
     */
    private void processOperation(CompletableOperation operation) throws Exception {
        Preconditions.checkState(!operation.isDone(), "The Operation has already been processed.");

        Operation entry = operation.getOperation();
        if (!entry.canSerialize()) {
            // This operation cannot be serialized, so don't bother doing anything with it.
            return;
        }

        synchronized (this.stateLock) {
            // Update Metadata and Operations with any missing data (offsets, lengths, etc) - the Metadata Updater
            // has all the knowledge for that task.
            this.metadataUpdater.preProcessOperation(entry);

            // Entry is ready to be serialized; assign a sequence number.
            entry.setSequenceNumber(this.metadataUpdater.nextOperationSequenceNumber());
            this.dataFrameBuilder.append(operation.getOperation());
            this.metadataUpdater.acceptOperation(entry);
        }

        log.trace("{}: DataFrameBuilder.Append {}.", this.traceObjectId, operation.getOperation());
    }

    /**
     * Closes the Operation Queue and fails all Operations in it with the given exception.
     *
     * @param causingException The exception to fail with. If null, it will default to ObjectClosedException.
     */
    private void closeQueue(Throwable causingException) {
        BlockingDrainingQueue<CompletableOperation> queue = this.operationQueue;
        if (queue != null) {
            // Close the queue and extract any outstanding Operations from it.
            Collection<CompletableOperation> remainingOperations = queue.close();
            if (remainingOperations != null && remainingOperations.size() > 0) {
                // If any outstanding Operations were left in the queue, they need to be failed.
                // If no other cause was passed, assume we are closing the queue because we are shutting down.
                Throwable failException = causingException != null ? causingException : new CancellationException();
                cancelIncompleteOperations(remainingOperations, failException);
            }
        }
    }

    /**
     * Cancels those Operations in the given list that have not yet completed with the given exception.
     */
    private void cancelIncompleteOperations(Iterable<CompletableOperation> operations, Throwable failException) {
        assert failException != null : "no exception to set";
        int cancelCount = 0;
        for (CompletableOperation o : operations) {
            if (!o.isDone()) {
                this.state.failOperation(o, failException);
                cancelCount++;
            }
        }

        log.warn("{}: Cancelling {} operations with exception: {}.", this.traceObjectId, cancelCount, failException.toString());
    }

    /**
     * Determines whether the given Throwable is a fatal exception from which we cannot recover.
     */
    private static boolean isFatalException(Throwable ex) {
        return ex instanceof DataCorruptionException
                || ex instanceof DataLogWriterNotPrimaryException
                || ex instanceof ObjectClosedException;
    }

    //endregion

    //region QueueProcessingState

    /**
     * Temporary State for the QueueProcessor. Keeps track of pending Operations and allows committing or failing all of them.
     * Note: this class shares state with OperationProcessor, as it accesses many of its private fields and uses its stateLock
     * for synchronization. Care should be taken if it is refactored out of here.
     */
    @ThreadSafe
    private class QueueProcessingState {
        @GuardedBy("stateLock")
        private final Deque<CompletableOperation> pendingOperations;
        private final MemoryStateUpdater logUpdater;
        private final MetadataCheckpointPolicy checkpointPolicy;
        @GuardedBy("stateLock")
        private final SortedDeque<DataFrameBuilder.CommitArgs> metadataTransactions;
        @GuardedBy("stateLock")
        private long highestCommittedDataFrame;

        private QueueProcessingState(MemoryStateUpdater stateUpdater, MetadataCheckpointPolicy checkpointPolicy) {
            this.logUpdater = Preconditions.checkNotNull(stateUpdater, "stateUpdater");
            this.checkpointPolicy = Preconditions.checkNotNull(checkpointPolicy, "checkpointPolicy");
            this.pendingOperations = new ArrayDeque<>();
            this.metadataTransactions = new SortedDeque<>();
            this.highestCommittedDataFrame = -1;
        }

        /**
         * Adds a new pending operation.
         *
         * @param operation The operation to append.
         */
        void addPending(CompletableOperation operation) {
            synchronized (stateLock) {
                this.pendingOperations.add(operation);
            }

            autoCompleteIfNeeded();
        }

        /**
         * Callback for when a DataFrame has been Sealed and is ready to be written to the DurableDataLog.
         * Seals the current metadata UpdateTransaction and maps it to the given CommitArgs. This UpdateTransaction
         * marks a point in the OperationMetadataUpdater that corresponds to the state of the Log at the end of the
         * DataFrame represented by the given commitArgs.
         *
         * @param commitArgs The CommitArgs to create a checkpoint for.
         */
        void frameSealed(DataFrameBuilder.CommitArgs commitArgs) {
            synchronized (stateLock) {
                commitArgs.setIndexKey(OperationProcessor.this.metadataUpdater.sealTransaction());
                this.metadataTransactions.addLast(commitArgs);
            }
        }

        /**
         * Callback for when a DataFrame has been successfully written to the DurableDataLog.
         * Commits all pending Metadata changes, assigns a TruncationMarker mapped to the given commitArgs and
         * acknowledges the pending operations up to the given commitArgs.
         *
         * It is important to note that this call is inclusive of all calls with arguments prior to it. It will
         * automatically complete all UpdateTransactions (and their corresponding operations) for all commitArgs that are
         * still registered but have a key smaller than the one in the given argument.
         *
         * @param commitArgs The Data Frame Commit Args that triggered this action.
         */
        void commit(DataFrameBuilder.CommitArgs commitArgs) {
            assert commitArgs.key() >= 0 : "DataFrameBuilder.CommitArgs does not have a key set";
            log.debug("{}: CommitSuccess ({}).", traceObjectId, commitArgs);

            List<CompletableOperation> toComplete = new ArrayList<>();
            Map<CompletableOperation, Throwable> toFail = new HashMap<>();
            try {
                // Record the end of a frame in the DurableDataLog directly into the base metadata. No need for locking here,
                // as the metadata has its own.
                OperationProcessor.this.metadata.recordTruncationMarker(commitArgs.getLastStartedSequenceNumber(), commitArgs.getLogAddress());
                final long lastOperationSequence = commitArgs.getLastFullySerializedSequenceNumber();
                final long addressSequence = commitArgs.getLogAddress().getSequence();

                synchronized (stateLock) {
                    if (addressSequence <= this.highestCommittedDataFrame) {
                        // Ack came out of order (we already processed one with a higher SeqNo).
                        log.debug("{}: CommitRejected ({}, HighestCommittedDataFrame = {}).", traceObjectId, commitArgs, this.highestCommittedDataFrame);
                        return;
                    }

                    if (state() != State.RUNNING) {
                        // We are shutting down.
                        log.debug("{}: CommitRejected ({}, Not Running, State = {}).", traceObjectId, commitArgs, state());
                        return;
                    }

                    // Commit any changes to the metadata.
                    boolean checkpointExists = this.metadataTransactions.removeLessThanOrEqual(commitArgs);
                    assert checkpointExists : "No Metadata UpdateTransaction found for " + commitArgs;
                    OperationProcessor.this.metadataUpdater.commit(commitArgs.key());

                    // Acknowledge all pending entries, in the order in which they are in the queue (ascending seq no).
                    while (!this.pendingOperations.isEmpty()
                            && this.pendingOperations.peekFirst().getOperation().getSequenceNumber() <= lastOperationSequence) {
                        CompletableOperation op = this.pendingOperations.pollFirst();
                        try {
                            this.logUpdater.process(op.getOperation());
                        } catch (Throwable ex) {
                            // MemoryStateUpdater.process() should only throw DataCorruptionExceptions, but just in case it
                            // throws something else (i.e. NullPtr), we still need to handle it.
                            // First, fail the operation, since it has already been taken off the pending list.
                            log.error("{}: OperationCommitFailure ({}). {}", traceObjectId, op.getOperation(), ex);
                            toFail.put(op, ex);

                            // Then fail the remaining operations (which also handles fatal errors) and bail out.
                            collectFailureCandidates(ex, commitArgs, toFail);
                            if (isFatalException(ex)) {
                                CallbackHelpers.invokeSafely(OperationProcessor.this::errorHandler, ex, null);
                            }

                            return;
                        }

                        Throwable stopException = OperationProcessor.this.getStopException();
                        if (stopException != null) {
                            // Even if the operation succeeded, we encountered a stop exception (fatal failure) and most likely
                            // this operation will be corrupted in the DurableDataLog. Do not ack it as success. If the owning
                            // Container does end up recovering successfully, it's up to the client to figure out the state
                            // of the affected segments so it can resume operations.
                            toFail.put(op, stopException);
                        } else {
                            toComplete.add(op);
                        }
                    }

                    this.highestCommittedDataFrame = addressSequence;
                }

                this.logUpdater.flush();
            } finally {
                toComplete.forEach(CompletableOperation::complete);
                toFail.forEach(this::failOperation);
                autoCompleteIfNeeded();
                if (toFail.size() == 0) {
                    // Only record the commit if we had no failures.
                    this.checkpointPolicy.recordCommit(commitArgs.getDataFrameLength());
                }
            }
        }

        /**
         * Callback for when a DataFrame has failed to be written to the DurableDataLog.
         * Rolls back pending Metadata changes that are mapped to the given commitArgs (and after) and fails all pending
         * operations that are affected.
         *
         * It is important to note that this call is inclusive of all calls with arguments after it. It will automatically
         * complete all UpdateTransactions (and their corresponding operations) for all commitArgs that are registered but
         * have a key larger than the one in the given argument.
         *
         * @param ex The cause of the failure. The operations will be failed with this as a cause.
         * @param commitArgs The Data Frame Commit Args that triggered this action.
         */
        void fail(Throwable ex, DataFrameBuilder.CommitArgs commitArgs) {
            Map<CompletableOperation, Throwable> toFail = new HashMap<>();
            try {
                synchronized (stateLock) {
                    collectFailureCandidates(ex, commitArgs, toFail);
                }
            } finally {
                toFail.forEach(this::failOperation);
                autoCompleteIfNeeded();
            }

            // All exceptions are final. If we cannot write to DurableDataLog, the safest way out is to shut down and
            // perform a new recovery that will detect any possible data loss or corruption.
            CallbackHelpers.invokeSafely(OperationProcessor.this::errorHandler, ex, null);
        }

        /**
         * Fails the given Operation either with the given failure cause, or with the general stop exception.
         *
         * @param operation    The CompletableOperation to fail.
         * @param failureCause The original failure cause. The operation will be failed with this exception, unless
         *                     the general stopException is set, in which case that takes precedence.
         */
        void failOperation(CompletableOperation operation, Throwable failureCause) {
            synchronized (stateLock) {
                Throwable stopException = OperationProcessor.this.getStopException();
                if (stopException != null) {
                    failureCause = stopException;
                }
            }

            operation.fail(failureCause);
        }

        /**
         * Rolls back any metadata that is affected by a failure for the given commit args and collects all CompletableOperations
         * off the pendingOperations queue that are affected. While the metadata is rolled back, the operations themselves
         * are not failed (since this method executes while holding the lock).
         *
         * @param ex         The Exception that triggered the failure.
         * @param commitArgs The CommitArgs that points to the DataFrame which failed to commit.
         * @param target     A Map where the failure candidates will be collected. Key=Operation, Value=Failure Exception.
         */
        @GuardedBy("stateLock")
        private void collectFailureCandidates(Throwable ex, DataFrameBuilder.CommitArgs commitArgs, Map<CompletableOperation, Throwable> target) {
            // Discard all updates to the metadata.
            long updateTransactionId = 0;
            if (commitArgs != null) {
                boolean checkpointExists = this.metadataTransactions.removeGreaterThanOrEqual(commitArgs);
                if (checkpointExists) {
                    updateTransactionId = commitArgs.key();
                }
            }

            if (updateTransactionId == 0) {
                this.metadataTransactions.clear();
            }

            OperationProcessor.this.metadataUpdater.rollback(updateTransactionId);

            // Fail all pending entries.
            long seqNo = this.metadataTransactions.isEmpty() ?
                    ContainerMetadata.INITIAL_OPERATION_SEQUENCE_NUMBER :
                    this.metadataTransactions.peekLast().getLastFullySerializedSequenceNumber();
            while (!this.pendingOperations.isEmpty() && shouldFail(this.pendingOperations.peekLast(), seqNo)) {
                target.put(this.pendingOperations.pollLast(), ex);
            }
        }

        /**
         * Determines whether to fail the CompletableOperation, given the SeqNo of the last known successfully committed
         * operation.
         */
        private boolean shouldFail(CompletableOperation co, long lastKnownSuccessfulSeqNo) {
            return !co.getOperation().canSerialize()
                    || co.getOperation().getSequenceNumber() > lastKnownSuccessfulSeqNo;
        }

        /**
         * Auto-completes any non-serialization operations at the beginning of the Pending Operations queue. Due to their
         * nature, these operations are at risk of never being completed, and, if there are no more pending operations
         * before that, they can be completed without further delay.
         */
        private void autoCompleteIfNeeded() {
            Collection<CompletableOperation> toComplete = null;
            synchronized (stateLock) {
                while (!this.pendingOperations.isEmpty() && !this.pendingOperations.peekFirst().getOperation().canSerialize()) {
                    if (toComplete == null) {
                        toComplete = new ArrayList<>();
                    }

                    toComplete.add(this.pendingOperations.pollFirst());
                }
            }

            if (toComplete != null) {
                toComplete.forEach(CompletableOperation::complete);
            }
        }
    }

    //endregion
}
