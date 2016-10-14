/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.logs;

import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.ObjectClosedException;
import com.emc.pravega.common.util.BlockingDrainingQueue;
import com.emc.pravega.service.server.Container;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.IllegalContainerStateException;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.logs.operations.CompletableOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.storage.DurableDataLog;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single-thread Processor for Operations. Queues all incoming entries in a BlockingDrainingQueue, then picks them all
 * at once, generates DataFrames from them and commits them to the DataFrameLog, one by one, in sequence.
 */
@Slf4j
class OperationProcessor extends AbstractExecutionThreadService implements Container {
    //region Members

    private final String traceObjectId;
    private final OperationMetadataUpdater metadataUpdater;
    private final MemoryLogUpdater logUpdater;
    private final DurableDataLog durableDataLog;
    private final BlockingDrainingQueue<CompletableOperation> operationQueue;
    private final MetadataCheckpointPolicy checkpointPolicy;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the OperationProcessor class.
     *
     * @param metadata         The ContainerMetadata for the Container to process operations for.
     * @param logUpdater       A MemoryLogUpdater that is used to update in-memory structures upon successful Operation committal.
     * @param durableDataLog   The DataFrameLog to write DataFrames to.
     * @param checkpointPolicy The Checkpoint Policy for Metadata.
     * @throws NullPointerException If any of the arguments are null.
     */
    OperationProcessor(UpdateableContainerMetadata metadata, MemoryLogUpdater logUpdater, DurableDataLog durableDataLog, MetadataCheckpointPolicy checkpointPolicy) {
        Preconditions.checkNotNull(metadata, "metadata");
        Preconditions.checkNotNull(logUpdater, "logUpdater");
        Preconditions.checkNotNull(durableDataLog, "durableDataLog");
        Preconditions.checkNotNull(checkpointPolicy, "checkpointPolicy");

        this.traceObjectId = String.format("OperationProcessor[%d]", metadata.getContainerId());
        this.metadataUpdater = new OperationMetadataUpdater(metadata);
        this.logUpdater = logUpdater;
        this.durableDataLog = durableDataLog;
        this.checkpointPolicy = checkpointPolicy;
        this.operationQueue = new BlockingDrainingQueue<>();
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            stopAsync();
            ServiceShutdownListener.awaitShutdown(this, false);
            log.info("{}: Closed.", this.traceObjectId);
            this.closed.set(true);
        }
    }

    //endregion

    //region AbstractExecutionThreadService Implementation

    @Override
    protected void run() throws Exception {
        long traceId = LoggerHelpers.traceEnter(log, traceObjectId, "run");
        Throwable closingException = null;
        try {
            while (isRunning()) {
                runOnce();
            }
        } catch (InterruptedException ex) {
            closingException = ex;
            if (state() != State.STOPPING) {
                // We only expect InterruptedException if we are in the process of Stopping. All others are indicative
                // of some failure.
                throw ex;
            }
        } catch (DataCorruptionException ex) {
            closingException = ex;
            throw ex;
        } finally {
            closeQueue(closingException);
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "run", traceId);
    }

    @Override
    protected void triggerShutdown() {
        // We are being told (externally) to stop. We need to first stop the queue, which will prevent any new items
        // from being processed, as well as stopping the main worker thread.
        closeQueue(null);
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
        if (!isRunning()) {
            throw new IllegalContainerStateException("OperationProcessor is not running.");
        }

        log.debug("{}: process {}.", this.traceObjectId, operation);
        CompletableFuture<Long> result = new CompletableFuture<>();
        this.operationQueue.add(new CompletableOperation(operation, result));
        return result;
    }

    //endregion

    //region Queue Processing

    /**
     * Single iteration of the Queue Processor.
     * Steps:
     * <ol>
     * <li> Picks all items from the Queue. If none, exits.
     * <li> Creates a DataFrameBuilder and starts appending items to it.
     * <li> As the DataFrameBuilder acknowledges DataFrames being published, acknowledge the corresponding Operations as well.
     * </ol>
     *
     * @throws InterruptedException    If the current thread has been interrupted (externally).
     * @throws DataCorruptionException If an invalid state of the Log or Metadata has been detected (which usually indicates corruption).
     */

    private void runOnce() throws DataCorruptionException, InterruptedException {
        List<CompletableOperation> operations = this.operationQueue.takeAllEntries();
        log.debug("{}: RunOnce (OperationCount = {}).", this.traceObjectId, operations.size());
        int currentIndex = 0;
        while (currentIndex < operations.size()) {
            QueueProcessingState state = null;
            try {
                // Resume processing operations from where we left off.
                // In the happy case, this loop is only executed once. But we need the bigger while loop in case we
                // encountered a non-fatal exception. There is no point in failing the whole set of operations if only
                // one set failed.
                state = new QueueProcessingState(this.metadataUpdater, this.logUpdater, this.checkpointPolicy, this.traceObjectId);
                DataFrameBuilder<Operation> dataFrameBuilder = new DataFrameBuilder<>(this.durableDataLog, state::commit, state::fail);
                for (; currentIndex < operations.size(); currentIndex++) {
                    CompletableOperation o = operations.get(currentIndex);
                    boolean processedSuccessfully = processOperation(o, dataFrameBuilder);

                    // Add the operation as 'pending', only if we were able to successfully append it to a data frame.
                    // We only commit data frames when we attempt to start a new record (if it's full) or if we try to close it, so we will not miss out on it.
                    if (processedSuccessfully) {
                        state.addPending(o);
                    }
                }

                // Only close the DataFrameBuilder (which means flush whatever we have in it to the DataLog) if everything
                // went well. If we had any exceptions, and we fail the State, then we must not flush anything out.
                dataFrameBuilder.close();
            } catch (Exception ex) {
                // Fail the current set of operations with the given exception. Unless a DataCorruptionException (see below),
                // we will proceed with the next batch of operations.
                Throwable realCause = ExceptionHelpers.getRealException(ex);
                if (state != null) {
                    state.fail(realCause);
                }

                if (realCause instanceof DataCorruptionException) {
                    // This is a nasty one. If we encountered a DataCorruptionException, it means we detected something abnormal
                    // in our container. We need to shutdown right away.

                    // But first, fail any Operations that we did not have a chance to process yet.
                    cancelIncompleteOperations(operations, realCause);
                    throw (DataCorruptionException) realCause;
                }
            }
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

        // Update Metadata and Operations with any missing data (offsets, lengths, etc) - the Metadata Updater has all the knowledge for that task.
        Operation entry = operation.getOperation();
        try {
            this.metadataUpdater.preProcessOperation(entry);
        } catch (Exception ex) {
            // This entry was not accepted (due to external error) or some processing error occurred. Our only option is to fail it now, before trying to commit it.
            operation.fail(ex);
            return false;
        }

        // Entry is ready to be serialized; assign a sequence number.
        entry.setSequenceNumber(this.metadataUpdater.nextOperationSequenceNumber());

        log.trace("{}: DataFrameBuilder.Append {}.", this.traceObjectId, operation.getOperation());
        try {
            dataFrameBuilder.append(operation.getOperation());
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

        try {
            this.metadataUpdater.acceptOperation(entry);
        } catch (MetadataUpdateException ex) {
            // This is an internal error. This shouldn't happen. The entry has been committed, but we couldn't update the metadata due to a bug.
            operation.fail(ex);
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
            List<CompletableOperation> remainingOperations = queue.close();
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
    private void cancelIncompleteOperations(List<CompletableOperation> operations, Throwable failException) {
        assert failException != null : "no exception to set";
        int cancelCount = 0;
        for (CompletableOperation o : operations) {
            if (!o.isDone()) {
                o.fail(failException);
                cancelCount++;
            }
        }

        log.debug("{}: Cancelling {} operations because with exception: {}.", this.traceObjectId, cancelCount, failException.toString());
    }

    //endregion

    //region QueueProcessingState

    /**
     * Temporary State for the QueueProcessor. Keeps track of pending Operations and allows committing or failing all of them.
     */
    @Slf4j
    private static class QueueProcessingState {
        private final String traceObjectId;
        private final LinkedList<CompletableOperation> pendingOperations;
        private final OperationMetadataUpdater metadataUpdater;
        private final MemoryLogUpdater logUpdater;
        private final MetadataCheckpointPolicy checkpointPolicy;

        QueueProcessingState(OperationMetadataUpdater metadataUpdater, MemoryLogUpdater logUpdater, MetadataCheckpointPolicy checkpointPolicy, String traceObjectId) {
            assert metadataUpdater != null : "metadataUpdater is null";
            assert logUpdater != null : "logUpdater is null";
            assert checkpointPolicy != null : "checkpointPolicy is null";

            this.traceObjectId = traceObjectId;
            this.pendingOperations = new LinkedList<>();
            this.metadataUpdater = metadataUpdater;
            this.logUpdater = logUpdater;
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
            while (this.pendingOperations.size() > 0 && this.pendingOperations.getFirst().getOperation().getSequenceNumber() <= commitArgs.getLastFullySerializedSequenceNumber()) {
                CompletableOperation e = this.pendingOperations.removeFirst();
                try {
                    this.logUpdater.process(e.getOperation());
                } catch (DataCorruptionException ex) {
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
    }

    //endregion
}
