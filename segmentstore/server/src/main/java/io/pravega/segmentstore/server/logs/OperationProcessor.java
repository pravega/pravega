/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.logs;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.AbstractThreadPoolService;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.BlockingDrainingQueue;
import io.pravega.common.util.PriorityBlockingDrainingQueue;
import io.pravega.segmentstore.server.CacheUtilizationProvider;
import io.pravega.segmentstore.server.IllegalContainerStateException;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.server.ServiceHaltException;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.logs.operations.CompletableOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationPriority;
import io.pravega.segmentstore.server.logs.operations.OperationSerializer;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.DurableDataLog;
import io.pravega.segmentstore.storage.cache.CacheFullException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
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
    private static final int MAX_COMMIT_QUEUE_SIZE = 50;
    private static final Duration PROCESSOR_TIMEOUT = Duration.ofSeconds(5);

    private final UpdateableContainerMetadata metadata;
    private final MemoryStateUpdater stateUpdater;
    @GuardedBy("stateLock")
    private final OperationMetadataUpdater metadataUpdater;
    private final PriorityBlockingDrainingQueue<CompletableOperation> operationQueue;
    private final BlockingDrainingQueue<List<CompletableOperation>> commitQueue;
    private final Object stateLock = new Object();
    private final QueueProcessingState state;
    @GuardedBy("stateLock")
    private final DataFrameBuilder<Operation> dataFrameBuilder;
    @Getter
    private final SegmentStoreMetrics.OperationProcessor metrics;
    private final Throttler throttler;
    private final CacheUtilizationProvider cacheUtilizationProvider;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the OperationProcessor class.
     *
     * @param metadata          The ContainerMetadata for the Container to process operations for.
     * @param stateUpdater      A MemoryStateUpdater that is used to update in-memory structures upon successful Operation committal.
     * @param durableDataLog    The DataFrameLog to write DataFrames to.
     * @param checkpointPolicy  The Checkpoint Policy for Metadata.
     * @param throttlerPolicy Configuration parameters for ThrottlerCalculator.
     * @param executor          An Executor to use for async operations.
     * @throws NullPointerException If any of the arguments are null.
     */
    OperationProcessor(UpdateableContainerMetadata metadata, MemoryStateUpdater stateUpdater, DurableDataLog durableDataLog,
                       MetadataCheckpointPolicy checkpointPolicy, ThrottlerPolicy throttlerPolicy, ScheduledExecutorService executor) {
        super(String.format("OperationProcessor[%d]", metadata.getContainerId()), executor);
        Preconditions.checkNotNull(durableDataLog, "durableDataLog");
        this.metadata = metadata;
        this.stateUpdater = Preconditions.checkNotNull(stateUpdater, "stateUpdater");
        this.metadataUpdater = new OperationMetadataUpdater(this.metadata);
        this.operationQueue = new PriorityBlockingDrainingQueue<>(OperationPriority.getMaxPriorityValue());
        this.commitQueue = new BlockingDrainingQueue<>();
        this.state = new QueueProcessingState(checkpointPolicy);
        val args = new DataFrameBuilder.Args(this.state::frameSealed, this.state::commit, this.state::fail, this.executor);
        this.dataFrameBuilder = new DataFrameBuilder<>(durableDataLog, OperationSerializer.DEFAULT, args);
        this.metrics = new SegmentStoreMetrics.OperationProcessor(this.metadata.getContainerId());
        this.cacheUtilizationProvider = stateUpdater.getCacheUtilizationProvider();
        val throttlerCalculator = ThrottlerCalculator
                .builder()
                .maxDelayMillis(throttlerPolicy.getMaxDelayMillis())
                .cacheThrottler(this.cacheUtilizationProvider::getCacheUtilization, this.cacheUtilizationProvider.getCacheTargetUtilization(),
                        this.cacheUtilizationProvider.getCacheMaxUtilization(), throttlerPolicy.getMaxDelayMillis())
                .batchingThrottler(durableDataLog::getQueueStatistics, throttlerPolicy.getMaxBatchingDelayMillis())
                .durableDataLogThrottler(durableDataLog.getWriteSettings(), durableDataLog::getQueueStatistics, throttlerPolicy.getMaxDelayMillis())
                .operationLogThrottler(this.stateUpdater::getInMemoryOperationLogSize, throttlerPolicy.getMaxDelayMillis(),
                        throttlerPolicy.getOperationLogMaxSize(), throttlerPolicy.getOperationLogTargetSize())
                .build();
        this.throttler = new Throttler(this.metadata.getContainerId(), throttlerCalculator, this::shouldSuspendThrottlingDelay, executor, this.metrics);
        this.cacheUtilizationProvider.registerCleanupListener(this.throttler);
        durableDataLog.registerQueueStateChangeListener(this.throttler);
        this.stateUpdater.registerReadListener(this.throttler);
    }

    //endregion

    //region AbstractThreadPoolService Implementation

    @Override
    protected Duration getShutdownTimeout() {
        return SHUTDOWN_TIMEOUT;
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        // The QueueProcessor is responsible with the processing of externally added Operations. It starts when the
        // OperationProcessor starts and is shut down as soon as doStop() is invoked.
        val queueProcessor = Futures
                .loop(this::isRunning,
                        () -> getThrottler().throttle()
                                .thenComposeAsync(v -> this.operationQueue.take(getFetchCount(), PROCESSOR_TIMEOUT, this.executor), this.executor)
                                .handleAsync((items, ex) -> handleProcessItems(items, this::processOperations, ex, "queueProcessor"), this.executor),
                        this.executor);

        // The CommitProcessor is responsible for the processing of those Operations that have already been committed to
        // DurableDataLog and now need to be added to the in-memory State.
        // As opposed from the QueueProcessor, this needs to process all pending commits and not discard them, even when
        // we receive a stop signal (from doStop()), otherwise we could be left with an inconsistent in-memory state.
        val commitProcessor = Futures
                .loop(() -> (isRunning() && !queueProcessor.isDone()) || this.commitQueue.size() > 0,
                        () -> this.commitQueue.take(MAX_COMMIT_QUEUE_SIZE, PROCESSOR_TIMEOUT, this.executor)
                                .handleAsync((items, ex) -> handleProcessItems(items, this::processCommits, ex, "commitProcessor"), this.executor),
                        this.executor)
                .whenComplete((r, ex) -> {
                    log.info("{}: Completing and closing commitProcessor. OperationProcessor running? {}, queueProcessor running? {}.",
                            this.traceObjectId, isRunning(), queueProcessor.isDone());
                    // The CommitProcessor is done. Safe to close its queue now, regardless of whether it failed or
                    // shut down normally.
                    val uncommittedOperations = this.commitQueue.close();

                    // Update the cacheUtilizationProvider with the fact that these operations are no longer pending for the cache.
                    uncommittedOperations.stream().flatMap(Collection::stream).forEach(this.state::notifyOperationCommitted);
                    if (ex != null) {
                        log.warn("{}: commitProcessor completed exceptionally {}.", this.traceObjectId, ex.toString());
                        throw new CompletionException(ex);
                    }
                });
        return CompletableFuture.allOf(queueProcessor, commitProcessor)
                .exceptionally(this::iterationErrorHandler);
    }

    @SneakyThrows
    private <T> Void handleProcessItems(T items, Consumer<T> callback, Throwable ex, String processorName) {
        // Check if there is an exception from taking elements from commitQueue. If we get a TimeoutException, it is
        // expected, so do nothing. If any other exception comes form the commitQueue, then re-throw.
        if (ex != null && Exceptions.unwrap(ex) instanceof TimeoutException) {
            return null;
        } else if (ex != null && !(Exceptions.unwrap(ex) instanceof TimeoutException)) {
            log.warn("{}: Unexpected exception in OperationProcessor while processing items for {}, rethrowing.", this.traceObjectId, processorName, ex);
            throw ex;
        }
        // No exceptions and we got some elements to process.
        callback.accept(items);
        return null;
    }

    @Override
    protected void doStop() {
        // We need to first stop the operation queue, which will prevent any new items from being processed.
        Throwable ex = new CancellationException("OperationProcessor is shutting down.");
        closeQueue(ex);

        // Close the DataFrameBuilder and cancel any operations caught in limbo.
        synchronized (this.stateLock) {
            this.dataFrameBuilder.close();
        }

        this.state.fail(ex, null);
        this.throttler.close();
        this.metrics.close();
        super.doStop();
    }

    @Override
    protected void errorHandler(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        closeQueue(ex);
        if (!isShutdownException(ex)) {
            // Shutdown exceptions means we are already stopping, so no need to do anything else. For all other cases,
            // record the failure and then stop the OperationProcessor.
            super.errorHandler(ex);

            // Run async to prevent deadlocks. closeQueue() above is the most important thing when shutting down as it
            // will prevent anything new from being added while also cancelling in-flight requests.
            this.executor.execute(this::stopAsync);
        }
    }

    @SneakyThrows
    private Void iterationErrorHandler(Throwable ex) {
        ex = Exceptions.unwrap(ex);
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

    //region Queue Processing

    /**
     * Processes the given Operation. This method returns when the given Operation has been added to the internal queue.
     *
     * @param operation The Operation to process.
     * @param priority  Operation Priority.
     * @return A CompletableFuture that, when completed, will indicate the Operation has finished processing. If the
     * Operation completed successfully, the Future will contain the Sequence Number of the Operation. If the Operation
     * failed, it will contain the exception that caused the failure.
     * @throws IllegalContainerStateException If the OperationProcessor is not running.
     */
    CompletableFuture<Void> process(Operation operation, OperationPriority priority) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        if (!isRunning()) {
            result.completeExceptionally(new IllegalContainerStateException("OperationProcessor is not running."));
        } else {
            log.debug("{}: process[{}] {}.", this.traceObjectId, priority, operation);
            try {
                // If the operationQueue is closed, it means that OperationProcessor needs to shut down. This may
                // require first interrupting any current delay, so the shutdown sequence can be executed.
                if (this.operationQueue.isClosed()) {
                    getThrottler().notifyThrottleSourceChanged();
                }
                this.operationQueue.add(new CompletableOperation(operation, priority, result));
            } catch (Throwable e) {
                if (Exceptions.mustRethrow(e)) {
                    throw e;
                }

                result.completeExceptionally(e);
            }

            // A throttle-exempt operation has just been added (these are time-critical operations, which must execute
            // right away). If there is a throttling delay in progress, we must abort it, otherwise this operation
            // may not get a chance to execute.
            if (priority.isThrottlingExempt()) {
                getThrottler().notifyThrottleSourceChanged();
            }
        }

        return result;
    }

    /**
     * Gets the maximum number of Operations to fetch from the operation queue. This is calculated based on the estimated
     * cache insertion capacity and its goal is to reduce the number of operations we have in flight as we near the
     * maximum configured cache capacity.
     *
     * @return The maximum number of Operations to fetch from the operation queue.
     */
    private int getFetchCount() {
        return Math.max(1, (int) (this.cacheUtilizationProvider.getCacheInsertionCapacity() * MAX_READ_AT_ONCE));
    }

    /**
     * Determines whether we have to notify the {@link Throttler} to suspend current delay. This can happen in two cases:
     * i) If the {@link #operationQueue} has been closed (e.g., Bookkeeper error) while experiencing a maximum delay;
     * ii) If the {@link #operationQueue} has any {@link CompletableOperation} with an {@link OperationPriority} that
     * requires an immediate execution (i.e., {@link OperationPriority#isThrottlingExempt()} is true).
     *
     * @return True if throttling needs to be suspended, false otherwise.
     */
    @VisibleForTesting
    protected boolean shouldSuspendThrottlingDelay() {
        if (this.operationQueue.isClosed()) {
            // The operationQueue has been closed. This means that any throttling delay should be interrupted and shut down.
            return true;
        }
        val o = this.operationQueue.peek();
        return o != null && o.getPriority().isThrottlingExempt();
    }

    /**
     * Gets the throttler.
     */
    @VisibleForTesting
    protected Throttler getThrottler() {
        return this.throttler;
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
        Timer processTimer = new Timer();
        int count = 0;
        while (!operations.isEmpty()) {
            try {
                // Process the current set of operations.
                while (!operations.isEmpty()) {
                    CompletableOperation o = operations.poll();
                    this.metrics.operationQueueWaitTime(o.getTimer().getElapsedMillis());
                    try {
                        processOperation(o);
                        this.state.addPending(o);
                        count++;
                    } catch (Throwable ex) {
                        ex = Exceptions.unwrap(ex);
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
                    // We have processed all operations in the queue: this is a good time to report metrics.
                    this.metrics.currentState(this.operationQueue.size() + count, this.state.getPendingCount());
                    this.metrics.processOperations(count, processTimer.getElapsedMillis());
                    processTimer = new Timer(); // Reset this timer since we may be pulling in new operations.
                    count = 0;
                    if (shouldSuspendThrottlingDelay() || !getThrottler().isThrottlingRequired()) {
                        // Only pull in new operations if we do not require throttling. If we do, we need to go back to
                        // the main OperationProcessor loop and delay processing the next batch of operations.
                        operations = this.operationQueue.poll(getFetchCount());
                    }

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
                ex = Exceptions.unwrap(ex);
                this.state.fail(ex, null);

                if (isFatalException(ex)) {
                    // If we encountered a fatal exception, it means we detected something that we cannot possibly recover from.
                    // We need to shutdown right away (this will be done by the main loop).

                    // But first, fail any Operations that we did not have a chance to process yet.
                    cancelIncompleteOperations(operations, ex);
                    throw Exceptions.sneakyThrow(ex);
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
        synchronized (this.stateLock) {
            // Update Metadata and Operations with any missing data (offsets, lengths, etc) - the Metadata Updater
            // has all the knowledge for that task.
            this.metadataUpdater.preProcessOperation(entry);

            // Entry is ready to be serialized; assign a sequence number.
            entry.setSequenceNumber(this.metadataUpdater.nextOperationSequenceNumber());
            this.dataFrameBuilder.append(entry);
            this.metadataUpdater.acceptOperation(entry);
        }

        log.trace("{}: DataFrameBuilder.Append {}.", this.traceObjectId, entry);
    }

    /**
     * Closes the Operation Queue and fails all Operations in it with the given exception.
     *
     * @param causingException The exception to fail with. If null, it will default to ObjectClosedException.
     */
    @VisibleForTesting
    void closeQueue(Throwable causingException) {
        // Close the operation queue and extract any outstanding Operations from it.
        Collection<CompletableOperation> remainingOperations = this.operationQueue.close();
        if (remainingOperations != null && remainingOperations.size() > 0) {
            // If any outstanding Operations were left in the queue, they need to be failed.
            // If no other cause was passed, assume we are closing the queue because we are shutting down.
            Throwable failException = causingException != null ? causingException : new CancellationException();
            cancelIncompleteOperations(remainingOperations, failException);
        }

        // The commit queue will auto-close when we are done and it itself is empty. We just need to unblock it in case
        // it was idle and waiting on a pending take() operation.
        this.commitQueue.cancelPendingTake();
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
        return ex instanceof ServiceHaltException       // Covers DataCorruptionException
                || ex instanceof DataLogWriterNotPrimaryException
                || ex instanceof ObjectClosedException
                || ex instanceof CacheFullException;
    }

    private void processCommits(Queue<List<CompletableOperation>> items) {
        try {
            do {
                Timer memoryCommitTimer = new Timer();
                this.stateUpdater.process(items.stream().flatMap(List::stream).map(CompletableOperation::getOperation).iterator(),
                        this.state::notifyOperationCommitted);
                this.metrics.memoryCommit(items.size(), memoryCommitTimer.getElapsed());
                this.metrics.reportOperationLogSize(this.stateUpdater.getInMemoryOperationLogSize(), this.metadata.getContainerId());
                items = this.commitQueue.poll(MAX_COMMIT_QUEUE_SIZE);
            } while (!items.isEmpty());
        } catch (Throwable ex) {
            // MemoryStateUpdater.process() should only throw DataCorruptionExceptions, but just in case it
            // throws something else (i.e. NullPtr), we still need to handle it.
            log.error("{}: MemoryStateUpdater.process failure.", traceObjectId, ex);

            // Then fail the remaining operations (which also handles fatal errors) and bail out.
            if (isFatalException(ex)) {
                Callbacks.invokeSafely(OperationProcessor.this::errorHandler, ex, null);
            }
        }
    }

    //endregion

    //region QueueProcessingState

    /**
     * Temporary State for the OperationProcessor. Keeps track of pending Operations and allows committing or failing all of them.
     * Note: this class shares state with OperationProcessor, as it accesses many of its private fields and uses its stateLock
     * for synchronization. Care should be taken if it is refactored out of here.
     */
    @ThreadSafe
    private class QueueProcessingState {
        @GuardedBy("stateLock")
        private ArrayList<CompletableOperation> nextFrameOperations;
        @GuardedBy("stateLock")
        private int pendingOperationCount;
        private final MetadataCheckpointPolicy checkpointPolicy;
        @GuardedBy("stateLock")
        private final ArrayDeque<DataFrameBuilder.CommitArgs> metadataTransactions;
        @GuardedBy("stateLock")
        private long highestCommittedDataFrame;

        private QueueProcessingState(MetadataCheckpointPolicy checkpointPolicy) {
            this.checkpointPolicy = Preconditions.checkNotNull(checkpointPolicy, "checkpointPolicy");
            this.nextFrameOperations = new ArrayList<>();
            this.metadataTransactions = new ArrayDeque<>();
            this.highestCommittedDataFrame = -1;
            this.pendingOperationCount = 0;
        }

        /**
         * Adds a new pending operation.
         *
         * @param operation The operation to append.
         */
        void addPending(CompletableOperation operation) {
            cacheUtilizationProvider.adjustPendingBytes(operation.getOperation().getCacheLength());
            synchronized (stateLock) {
                this.nextFrameOperations.add(operation);
                this.pendingOperationCount++;
            }
        }

        /**
         * Records the fact that the given {@link CompletableOperation} is no longer pending (it has either been committed
         * or rejected) and as such, we need to subtract it from the Cache Utilization Provider's accounting.
         *
         * @param o The operation.
         */
        void notifyOperationCommitted(CompletableOperation o) {
            cacheUtilizationProvider.adjustPendingBytes(-o.getOperation().getCacheLength());
        }

        /**
         * Records the fact that the given {@link Operation} is no longer pending (it has either been committed
         * or rejected) and as such, we need to subtract it from the Cache Utilization Provider's accounting.
         *
         * @param o The operation.
         */
        void notifyOperationCommitted(Operation o) {
            cacheUtilizationProvider.adjustPendingBytes(-o.getCacheLength());
        }

        /**
         * Gets a value indicating the number of pending operations
         *
         * @return The count.
         */
        int getPendingCount() {
            synchronized (stateLock) {
                return this.pendingOperationCount;
            }
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
                commitArgs.setMetadataTransactionId(OperationProcessor.this.metadataUpdater.sealTransaction());
                commitArgs.setOperations(Collections.unmodifiableList(this.nextFrameOperations));
                this.nextFrameOperations = new ArrayList<>();
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
            assert commitArgs.getMetadataTransactionId() >= 0 : "DataFrameBuilder.CommitArgs does not have a key set";
            log.debug("{}: CommitSuccess ({}).", traceObjectId, commitArgs);
            Timer timer = new Timer();

            List<List<CompletableOperation>> toAck = null;
            try {
                // Record the end of a frame in the DurableDataLog directly into the base metadata. No need for locking here,
                // as the metadata has its own.
                OperationProcessor.this.metadata.recordTruncationMarker(commitArgs.getLastStartedSequenceNumber(), commitArgs.getLogAddress());
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

                    // Collect operations to commit.
                    toAck = collectCompletionCandidates(commitArgs);

                    // Commit metadata updates.
                    OperationProcessor.this.metadataUpdater.commit(commitArgs.getMetadataTransactionId());

                    // Queue operations for memory commit, which will be done asynchronously.
                    queueMemoryCommit(toAck);
                    this.highestCommittedDataFrame = addressSequence;
                }
            } finally {
                if (toAck != null) {
                    toAck.stream().flatMap(Collection::stream).forEach(CompletableOperation::complete);
                    metrics.operationsCompleted(toAck, timer.getElapsed());
                }
                this.checkpointPolicy.recordCommit(commitArgs.getDataFrameLength());
            }
        }

        private void queueMemoryCommit(List<List<CompletableOperation>> toAck) {
            try {
                toAck.forEach(OperationProcessor.this.commitQueue::add);
            } catch (Exception ex) {
                // If we are unable to queue up these operations for whatever reason, we must unregister them from the
                // CacheUtilizationProvider, otherwise we will be "leaking" bytes (i.e., overcounting).
                toAck.forEach(l -> l.forEach(this::notifyOperationCommitted));
                throw ex;
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
            List<CompletableOperation> toFail = null;
            try {
                synchronized (stateLock) {
                    toFail = collectFailureCandidates(commitArgs);
                    this.pendingOperationCount -= toFail.size();
                }
            } finally {
                if (toFail != null) {
                    toFail.forEach(o -> {
                        failOperation(o, ex);
                        notifyOperationCommitted(o);
                    });
                    metrics.operationsFailed(toFail);
                }
            }

            // All exceptions are final. If we cannot write to DurableDataLog, the safest way out is to shut down and
            // perform a new recovery that will detect any possible data loss or corruption.
            Callbacks.invokeSafely(OperationProcessor.this::errorHandler, ex, null);
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
         * Collects all Operations that have been successfully committed to DurableDataLog and removes the associated
         * Metadata Update Transactions for those commits. The operations themselves are not completed (since we are
         * holding a lock) and the Metadata is not updated while executing this method.
         *
         * @param commitArgs The CommitArgs that points to the DataFrame which successfully committed.
         * @return An ordered List of ordered Lists of Operations that have been committed.
         */
        @GuardedBy("stateLock")
        private List<List<CompletableOperation>> collectCompletionCandidates(DataFrameBuilder.CommitArgs commitArgs) {
            List<List<CompletableOperation>> toAck = new ArrayList<>();
            long transactionId = commitArgs.getMetadataTransactionId();
            boolean checkpointExists = false;
            while (!this.metadataTransactions.isEmpty() && this.metadataTransactions.peekFirst().getMetadataTransactionId() <= transactionId) {
                DataFrameBuilder.CommitArgs t = this.metadataTransactions.pollFirst();
                checkpointExists |= t.getMetadataTransactionId() == transactionId;
                if (t.getOperations().size() > 0) {
                    toAck.add(t.getOperations());
                    this.pendingOperationCount -= t.getOperations().size();
                }
            }

            if (!checkpointExists) {
                // Under normal circumstances, there should always be an UpdateTransaction that matches our argument;
                // however if we had just processed a failure, collectFailureCandidates() may have cleared (and failed)
                // all of them, so, only in that case, would it be OK not to find one.
                log.warn("{}: No Metadata UpdateTransaction found for '{}' (Count={}). This is expected after a critical failure or when OperationProcessor is shutting down.",
                        traceObjectId, this.metadataTransactions.size(), commitArgs);

                // If a failure did happen, then there should be no other entries in metadataTransactions at this point.
                assert this.metadataTransactions.isEmpty() : "No Metadata UpdateTransaction found for given CommitArgs, "
                        + "but there are still entries in metadataTransaction.";
            }
            return toAck;
        }

        /**
         * Rolls back any metadata that is affected by a failure for the given commit args and collects all pending
         * CompletableOperations that are affected. While the metadata is rolled back, the operations themselves
         * are not failed (since this method executes while holding the lock).
         *
         * @param commitArgs The CommitArgs that points to the DataFrame which failed to commit.
         * @return A List where the failure candidates will be collected.
         */
        @GuardedBy("stateLock")
        private List<CompletableOperation> collectFailureCandidates(DataFrameBuilder.CommitArgs commitArgs) {
            // Discard all updates to the metadata.
            List<CompletableOperation> candidates = new ArrayList<>();
            if (commitArgs != null) {
                // Rollback all changes to the metadata from this commit on, and fail all involved operations.
                OperationProcessor.this.metadataUpdater.rollback(commitArgs.getMetadataTransactionId());
                while (!this.metadataTransactions.isEmpty() && this.metadataTransactions.peekLast().getMetadataTransactionId() >= commitArgs.getMetadataTransactionId()) {
                    // Fail all operations in this particular commit.
                    DataFrameBuilder.CommitArgs t = this.metadataTransactions.pollLast();
                    candidates.addAll(t.getOperations());
                }
            } else {
                // Rollback all changes to the metadata and fail all outstanding commits.
                this.metadataTransactions.forEach(t -> candidates.addAll(t.getOperations()));
                this.metadataTransactions.clear();
                OperationProcessor.this.metadataUpdater.rollback(0);
            }

            candidates.addAll(this.nextFrameOperations);
            this.nextFrameOperations.clear();
            return candidates;
        }
    }

    //endregion
}
