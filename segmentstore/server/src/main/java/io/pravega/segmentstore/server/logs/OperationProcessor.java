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
    private DataFrameBuilder<Operation> dataFrameBuilder;

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
        this.dataFrameBuilder = null;
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
        closeQueue(null);

        // Then close the DataFrameBuilder, which will await any pending data frames to be flushed away.
        val builder = getDataFrameBuilder(false);
        if (builder != null) {
            builder.close();
        }

        super.doStop();
    }

    @Override
    protected void errorHandler(Throwable ex) {
        ex = ExceptionHelpers.getRealException(ex);
        closeQueue(ex);
        if (!(ex instanceof CancellationException)) {
            // CancellationException means we are already stopping, so no need to do anything else. For all other cases,
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
        boolean isExpected = ex instanceof ObjectClosedException && (s == State.STOPPING || s == State.TERMINATED || s == State.FAILED);
        if (!isExpected) {
            throw ex;
        }

        return null;
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

    private DataFrameBuilder<Operation> getDataFrameBuilder(boolean recover) {
        synchronized (this.stateLock) {
            if (this.dataFrameBuilder == null || this.dataFrameBuilder.failureCause() != null) {
                // Builder is not created or in a failed state.
                if (recover) {
                    // If instructed to recover, recreate a new one.
                    val args = new DataFrameBuilder.Args(this.state::checkpoint, this.state::commit, this.state::fail, this.executor);
                    this.dataFrameBuilder = new DataFrameBuilder<>(this.durableDataLog, args);
                } else {
                    this.dataFrameBuilder = null;
                }
            }

            return this.dataFrameBuilder;
        }
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
                // Get the DataFrameBuilder, and recover it if necessary.
                val builder = getDataFrameBuilder(true);

                // Process the current set of operations.
                while (!operations.isEmpty()) {
                    CompletableOperation o = operations.poll();
                    if (processOperation(o, builder)) {
                        // Add the operation as 'pending', only if we were able to successfully append it to a data frame.
                        // We only commit data frames when we attempt to start a new record (if it's full) or if we try to
                        // close it, so we will not miss out on it.
                        this.state.addPending(o);
                    }
                }

                // Check if there are more operations to process. If so, it's more efficient to process them now (no thread
                // context switching, better DataFrame occupancy optimization) rather than by going back to run().
                if (operations.isEmpty()) {
                    operations = this.operationQueue.poll(MAX_READ_AT_ONCE);
                    if (operations.isEmpty()) {
                        log.debug("{}: processOperations (Flush).", this.traceObjectId);
                        synchronized (this.stateLock) {
                            builder.flush();
                        }
                    } else {
                        log.debug("{}: processOperations (Add OperationCount = {}).", this.traceObjectId, operations.size());
                    }
                }
            } catch (Throwable ex) {
                // Fail ALL the operations that haven't been checkpointed yet.
                Throwable realCause = ExceptionHelpers.getRealException(ex);
                this.state.fail(realCause, null);

                if (isFatalException(realCause)) {
                    // If we encountered a fatal exception, it means we detected something that we cannot possibly recover from.
                    // We need to shutdown right away (this will be done by the main loop).

                    // But first, fail any Operations that we did not have a chance to process yet.
                    cancelIncompleteOperations(operations, realCause);
                    throw Lombok.sneakyThrow(realCause);
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
     * If the Operation is rejected by the Metadata (due to the Operation being invalid or invalid for the current state
     * of the Segment(s) affected), this method will return false and the Operation will not be processed.
     *
     * If the Operation cannot be processed due to an exception, it will be rejected and the causing exception thrown.
     * This can include the underlying DataFrameBuilder being in a failed/closed state, or a DataCorruptionException.
     *
     * @param operation        The operation to process.
     * @param dataFrameBuilder The DataFrameBuilder to append the operation to.
     * @return True if processed successfully, false otherwise (false only if it got rejected due to it being invalid).
     * See @throws below for expected Exceptions.
     * @throws DataCorruptionException If a fatal exception that could possibly lead to corruption was detected along the
     * way. This is usually a terminal exception and there is no way to recover from it.
     * @throws ObjectClosedException If the given DataFrameBuilder is in a closed/failed state. In this case, the cause
     * of the ObjectClosedException needs to be inspected to determine the actual error that triggered this. This error
     * can generally be recovered from if a new DataFrameBuilder is created, however this method does not have enough
     * context to do so (and as such it is the responsibility of the upstream code to recover it).
     */
    private boolean processOperation(CompletableOperation operation, DataFrameBuilder<Operation> dataFrameBuilder) throws DataCorruptionException {
        Preconditions.checkState(!operation.isDone(), "The Operation has already been processed.");

        Operation entry = operation.getOperation();
        if (!entry.canSerialize()) {
            // This operation cannot be serialized, so don't bother doing anything with it.
            return true;
        }

        try {
            synchronized (this.stateLock) {
                // Update Metadata and Operations with any missing data (offsets, lengths, etc) - the Metadata Updater
                // has all the knowledge for that task.
                this.metadataUpdater.preProcessOperation(entry);

                // Entry is ready to be serialized; assign a sequence number.
                entry.setSequenceNumber(this.metadataUpdater.nextOperationSequenceNumber());
                dataFrameBuilder.append(operation.getOperation());
                this.metadataUpdater.acceptOperation(entry);
            }

            log.trace("{}: DataFrameBuilder.Append {}.", this.traceObjectId, operation.getOperation());
        } catch (ObjectClosedException ex) {
            // DataFrameBuilder is in a closed/failed state. We cannot proceed with this operation, or any other one,
            // so we need to re-throw the exception for it to be handled upstream.
            Throwable rootCause = dataFrameBuilder.failureCause();
            if (rootCause != null && rootCause != ex) {
                ex.addSuppressed(rootCause);
            }

            this.state.failOperation(operation, ex);
            throw ex;
        } catch (Exception ex) {
            this.state.failOperation(operation, ex);

            Throwable cause = ExceptionHelpers.getRealException(ex);
            if (cause instanceof DataCorruptionException) {
                // Besides failing the operation, DataCorruptionExceptions are pretty serious. We should shut down the
                // Operation Processor if we ever encounter one. The shutdown is handled in the main processor loop of
                // this class - which is responsible for determining whether the current iteration's error is a "fatal"
                // one or not.
                throw (DataCorruptionException) cause;
            }

            return false;
        }

        return true;
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
                Throwable failException = causingException != null ? causingException : new ObjectClosedException(this);
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
                || ex instanceof DataLogWriterNotPrimaryException;
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
         * Creates a checkpoint keyed in with the given CommitArgs. This indicates that the DataFrame represented
         * by the given args has been sealed and is about to be committed. This checkpoint will mark a point in the
         * OperationMetadataUpdater that corresponds to the end of that DataFrame.
         *
         * @param commitArgs The CommitArgs to create a checkpoint for.
         */
        void checkpoint(DataFrameBuilder.CommitArgs commitArgs) {
            synchronized (stateLock) {
                commitArgs.setIndexKey(OperationProcessor.this.metadataUpdater.sealTransaction());
                this.metadataTransactions.addLast(commitArgs);
            }
        }

        /**
         * Commits all pending Metadata changes, assigns a TruncationMarker and acknowledges all the pending operations.
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
                        log.debug("{}: CommitRejected ({}, HighestCommittedDataFrame = ).", traceObjectId, commitArgs, this.highestCommittedDataFrame);
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
         * Rolls back all pending Metadata changes and fails all pending operations.
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

            if (isFatalException(ex)) {
                CallbackHelpers.invokeSafely(OperationProcessor.this::errorHandler, ex, null);
            }
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
