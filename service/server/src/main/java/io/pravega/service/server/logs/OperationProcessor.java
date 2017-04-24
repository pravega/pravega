/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.logs;

import io.pravega.common.ExceptionHelpers;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.AbstractThreadPoolService;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.common.util.BlockingDrainingQueue;
import io.pravega.service.server.Container;
import io.pravega.service.server.DataCorruptionException;
import io.pravega.service.server.IllegalContainerStateException;
import io.pravega.service.server.UpdateableContainerMetadata;
import io.pravega.service.server.logs.operations.CompletableOperation;
import io.pravega.service.server.logs.operations.Operation;
import io.pravega.service.storage.DurableDataLog;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Single-thread Processor for Operations. Queues all incoming entries in a BlockingDrainingQueue, then picks them all
 * at once, generates DataFrames from them and commits them to the DataFrameLog, one by one, in sequence.
 */
@Slf4j
class OperationProcessor extends AbstractThreadPoolService implements Container {
    //region Members

    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private static final int MAX_READ_AT_ONCE = 1000;

    private final OperationMetadataUpdater metadataUpdater;
    private final MemoryStateUpdater stateUpdater;
    private final DurableDataLog durableDataLog;
    private final BlockingDrainingQueue<CompletableOperation> operationQueue;
    private final MetadataCheckpointPolicy checkpointPolicy;

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

        // No need to check metadata or executor != null as the super() call above takes care of that.
        Preconditions.checkNotNull(stateUpdater, "stateUpdater");
        Preconditions.checkNotNull(durableDataLog, "durableDataLog");
        Preconditions.checkNotNull(checkpointPolicy, "checkpointPolicy");

        this.metadataUpdater = new OperationMetadataUpdater(metadata);
        this.stateUpdater = stateUpdater;
        this.durableDataLog = durableDataLog;
        this.checkpointPolicy = checkpointPolicy;
        this.operationQueue = new BlockingDrainingQueue<>();
    }

    //endregion

    //region AbstractThreadPoolService Implementation

    @Override
    protected Duration getShutdownTimeout() {
        return SHUTDOWN_TIMEOUT;
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        return FutureHelpers.loop(
                this::isRunning,
                () -> this.operationQueue
                        .take(MAX_READ_AT_ONCE)
                        .thenAcceptAsync(this::processOperations, this.executor),
                this.executor);
    }

    @Override
    protected void doStop() {
        // We need to first stop the queue, which will prevent any new items from being processed.
        closeQueue(null);
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

    //endregion

    //region Container Implementation

    @Override
    public int getId() {
        return this.metadataUpdater.getContainerId();
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
            } catch (ObjectClosedException e) {
                result.completeExceptionally(e);
            }
        }

        return result;
    }

    //endregion

    //region Queue Processing

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

        // Create a new State and Builder (we need this either initially or after recovery from an error).
        final QueueProcessingState state = new QueueProcessingState(this.metadataUpdater, this.stateUpdater, this.checkpointPolicy, this.traceObjectId);
        final DataFrameBuilder<Operation> dataFrameBuilder = new DataFrameBuilder<>(this.durableDataLog, state::commit, state::fail);

        try {
            // Process the operations in the queue. This loop will ensure we continue processing after a recoverable failure,
            // as well as after we processed the entire collection, but found more items in need of processing.
            while (!operations.isEmpty()) {
                // Process the current set of operations.
                processOperations(operations, state, dataFrameBuilder);

                // Check if there are more operations to process. If so, it's more efficient to process them now (no thread
                // context switching, better DataFrame occupancy optimization) rather than by going back to run().
                if (operations.isEmpty()) {
                    operations = this.operationQueue.poll(MAX_READ_AT_ONCE);
                    log.debug("{}: processOperations (Add OperationCount = {}).", this.traceObjectId, operations.size());
                }
            }

            // Close the DataFrameBuilder, which makes sure that the last set of operations are properly flushed and
            // completed.
            dataFrameBuilder.close();
            if (state.hasPending()) {
                // Usually we reach this state if the only operation we had as a ProbeOperation (i.e. non-serializable),
                // which wouldn't have triggered a state.commit on its own.
                completeNonSerializableOperations(state);
            }
        } catch (Throwable ex) {
            handleIterationException(ex, state, operations);
        }
    }

    /**
     * Processes all the given operations, in order, using the given QueueProcessingState and DataFrameBuilder.
     *
     * @param operations       The operations to process.
     * @param state            The QueueProcessingState to use.
     * @param dataFrameBuilder The DataFrameBuilder to use for constructing DataFrames.
     */
    private void processOperations(Queue<CompletableOperation> operations, QueueProcessingState state, DataFrameBuilder<Operation> dataFrameBuilder) {
        try {
            while (!operations.isEmpty()) {
                CompletableOperation o = operations.poll();
                if (processOperation(o, dataFrameBuilder)) {
                    // Add the operation as 'pending', only if we were able to successfully append it to a data frame.
                    // We only commit data frames when we attempt to start a new record (if it's full) or if we try to
                    // close it, so we will not miss out on it.
                    state.addPending(o);
                }
            }
        } catch (Throwable ex) {
            handleIterationException(ex, state, operations);
            dataFrameBuilder.reset();
        }
    }

    /**
     * Processes a single operation.
     * Steps:
     * <ol>
     * <li> Pre-processes operation (in MetadataUpdater)
     * <li> Assigns Sequence Number
     * <li> Appends to DataFrameBuilder
     * <li> Accepts operation in MetadataUpdater.
     * </ol>
     * Any exceptions along the way will result in the immediate failure of the operations. Exceptions do not bubble out
     * of this method. The only way to determine whether the operation completed normally or not is to inspect the result.
     *
     * @param operation        The operation to process.
     * @param dataFrameBuilder The DataFrameBuilder to append the operation to.
     * @return True if processed successfully, false otherwise.
     */
    private boolean processOperation(CompletableOperation operation, DataFrameBuilder<Operation> dataFrameBuilder) throws DataCorruptionException {
        Preconditions.checkState(!operation.isDone(), "The Operation has already been processed.");

        Operation entry = operation.getOperation();
        if (!entry.canSerialize()) {
            // This operation cannot be serialized, so don't bother doing anything with it.
            return true;
        }

        try {
            // Update Metadata and Operations with any missing data (offsets, lengths, etc) - the Metadata Updater has all the knowledge for that task.
            this.metadataUpdater.preProcessOperation(entry);

            // Entry is ready to be serialized; assign a sequence number.
            entry.setSequenceNumber(this.metadataUpdater.nextOperationSequenceNumber());

            log.trace("{}: DataFrameBuilder.Append {}.", this.traceObjectId, operation.getOperation());
            dataFrameBuilder.append(operation.getOperation());
            this.metadataUpdater.acceptOperation(entry);
        } catch (Exception ex) {
            operation.fail(ex);
            Throwable cause = ExceptionHelpers.getRealException(ex);
            if (cause instanceof DataCorruptionException) {
                // Besides failing the operation, DataCorruptionExceptions are pretty serious. We should shut down the
                // Operation Processor if we ever encounter one.
                throw (DataCorruptionException) cause;
            }

            return false;
        }

        return true;
    }

    /**
     * Completes all operations in the given state that are non-serializable (if any are left).
     */
    private void completeNonSerializableOperations(QueueProcessingState state) {
        state.forEachPending(op -> {
            boolean canComplete = !op.getOperation().canSerialize();
            if (canComplete) {
                op.complete();
            }

            return canComplete;
        });

        // We need to ensure that the only possible pending operations are those that are non-serializable; otherwise
        // we have a problem.
        assert !state.hasPending() : "QueueProcessingState still has pending items after closing the DataFrameBuilder.";
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
                o.fail(failException);
                cancelCount++;
            }
        }

        log.warn("{}: Cancelling {} operations with exception: {}.", this.traceObjectId, cancelCount, failException.toString());
    }

    @SneakyThrows(DataCorruptionException.class)
    private void handleIterationException(Throwable ex, QueueProcessingState state, Collection<CompletableOperation> operations) {
        // Fail the current set of operations with the caught exception.
        Throwable realCause = ExceptionHelpers.getRealException(ex);
        state.fail(realCause);

        if (realCause instanceof DataCorruptionException) {
            // This is a nasty one. If we encountered a DataCorruptionException, it means we detected something abnormal
            // in our container. We need to shutdown right away.

            // But first, fail any Operations that we did not have a chance to process yet.
            cancelIncompleteOperations(operations, realCause);
            throw (DataCorruptionException) realCause;
        }
    }

    //endregion

    //region QueueProcessingState

    /**
     * Temporary State for the QueueProcessor. Keeps track of pending Operations and allows committing or failing all of them.
     */
    @Slf4j
    private static class QueueProcessingState {
        private final String traceObjectId;
        private final Queue<CompletableOperation> pendingOperations;
        private final OperationMetadataUpdater metadataUpdater;
        private final MemoryStateUpdater logUpdater;
        private final MetadataCheckpointPolicy checkpointPolicy;

        QueueProcessingState(OperationMetadataUpdater metadataUpdater, MemoryStateUpdater stateUpdater, MetadataCheckpointPolicy checkpointPolicy, String traceObjectId) {
            assert metadataUpdater != null : "metadataUpdater is null";
            assert stateUpdater != null : "stateUpdater is null";
            assert checkpointPolicy != null : "checkpointPolicy is null";

            this.traceObjectId = traceObjectId;
            this.pendingOperations = new LinkedList<>();
            this.metadataUpdater = metadataUpdater;
            this.logUpdater = stateUpdater;
            this.checkpointPolicy = checkpointPolicy;
        }

        /**
         * Adds a new pending operation.
         *
         * @param operation The operation to append.
         */
        void addPending(CompletableOperation operation) {
            this.pendingOperations.add(operation);
        }

        /**
         * Gets a value indicating whether there exist any pending operations in this state.
         */
        boolean hasPending() {
            return !this.pendingOperations.isEmpty();
        }

        /**
         * Commits all pending Metadata changes, assigns a TruncationMarker and acknowledges all the pending operations.
         *
         * @param commitArgs The Data Frame Commit Args that triggered this action.
         * @throws DataCorruptionException When the operation has been committed, but failed to be accepted into the In-Memory log.
         */
        public void commit(DataFrameBuilder.DataFrameCommitArgs commitArgs) throws Exception {
            log.debug("{}: CommitSuccess (OperationCount = {}).", this.traceObjectId, this.pendingOperations.size());

            // Record the Truncation marker and then commit any changes to metadata.
            this.metadataUpdater.recordTruncationMarker(commitArgs.getLastStartedSequenceNumber(), commitArgs.getLogAddress());
            this.metadataUpdater.commit();

            // Acknowledge all pending entries, in the order in which they are in the queue. It is important that we ack entries in order of increasing Sequence Number.
            while (this.pendingOperations.size() > 0 && this.pendingOperations.peek().getOperation().getSequenceNumber() <= commitArgs.getLastFullySerializedSequenceNumber()) {
                CompletableOperation e = this.pendingOperations.poll();
                try {
                    this.logUpdater.process(e.getOperation());
                } catch (Throwable ex) {
                    log.error("{}: OperationCommitFailure ({}). {}", this.traceObjectId, e.getOperation(), ex);
                    e.fail(ex);
                    throw ex;
                }

                e.complete();
            }

            this.logUpdater.flush();
            this.checkpointPolicy.recordCommit(commitArgs.getDataFrameLength());
        }

        /**
         * Rolls back all pending Metadata changes and fails all pending operations.
         *
         * @param ex The cause of the failure. The operations will be failed with this as a cause.
         */
        public void fail(Throwable ex) {
            log.error("{}: CommitFailure ({} operations). {}", this.traceObjectId, this.pendingOperations.size(), ex);

            // Discard all updates to the metadata.
            this.metadataUpdater.rollback();

            // Fail all pending entries.
            this.pendingOperations.forEach(e -> e.fail(ex));
            this.pendingOperations.clear();
        }

        public void forEachPending(Predicate<CompletableOperation> inspector) {
            this.pendingOperations.removeIf(inspector);
        }
    }

    //endregion
}
