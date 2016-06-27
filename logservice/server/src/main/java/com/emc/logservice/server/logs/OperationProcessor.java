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

package com.emc.logservice.server.logs;

import com.emc.logservice.common.BlockingDrainingQueue;
import com.emc.logservice.common.LoggerHelpers;
import com.emc.logservice.server.Container;
import com.emc.logservice.server.DataCorruptionException;
import com.emc.logservice.server.ExceptionHelpers;
import com.emc.logservice.server.logs.operations.CompletableOperation;
import com.emc.logservice.server.logs.operations.Operation;
import com.emc.logservice.server.logs.operations.StorageOperation;
import com.emc.logservice.storageabstraction.DurableDataLog;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Single-thread Processor for Operations. Queues all incoming entries in a BlockingDrainingQueue, then picks them all
 * at once, generates DataFrames from them and commits them to the DataFrameLog, one by one, in sequence.
 */
@Slf4j
public class OperationProcessor extends AbstractExecutionThreadService implements Container {
    //region Members

    private final String traceObjectId;
    private final String containerId;
    private final OperationMetadataUpdater metadataUpdater;
    private final MemoryLogUpdater logUpdater;
    private final DurableDataLog durableDataLog;
    private BlockingDrainingQueue<CompletableOperation> operationQueue;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the OperationProcessor class.
     *
     * @param containerId     The Id of the container this QueueProcessor belongs to.
     * @param metadataUpdater An OperationMetadataUpdater to work with.
     * @param logUpdater      A MemoryLogUpdater that is used to update in-memory structures upon successful Operation committal.
     * @param durableDataLog  The DataFrameLog to write DataFrames to.
     * @throws NullPointerException If any of the arguments are null.
     */
    public OperationProcessor(String containerId, OperationMetadataUpdater metadataUpdater, MemoryLogUpdater logUpdater, DurableDataLog durableDataLog) {
        Preconditions.checkNotNull(containerId, "containerId");
        Preconditions.checkNotNull(metadataUpdater, "metadataUpdater");
        Preconditions.checkNotNull(logUpdater, "logUpdater");
        Preconditions.checkNotNull(durableDataLog, "durableDataLog");

        this.traceObjectId = String.format("OperationProcessor[%s]", containerId);
        this.containerId = containerId;
        this.metadataUpdater = metadataUpdater;
        this.logUpdater = logUpdater;
        this.durableDataLog = durableDataLog;
    }

    //endregion

    //region AutoCloseable implementation

    @Override
    public void close() {
        this.stopAsync().awaitTerminated();
    }

    //endregion

    //region AbstractExecutionThreadService Implementation

    @Override
    protected void run() throws Exception {
        int traceId = LoggerHelpers.traceEnter(log, traceObjectId, "run");
        try {
            this.operationQueue = new BlockingDrainingQueue<>();
            while (this.isRunning()) {
                runOnce();
            }
        } catch (InterruptedException ex) {
            if (state() != State.STOPPING) {
                // We only expect InterruptedException if we are in the process of Stopping. All others are indicative
                // of some failure.
                throw ex;
            }
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "run", traceId);
    }

    @Override
    protected void triggerShutdown() {
        // We are being told (externally) to stop. We need to first stop the queue, which will prevent any new items
        // from being processed, as well as stopping the main worker thread.
        BlockingDrainingQueue<CompletableOperation> queue = this.operationQueue;
        if (queue != null) {
            queue.close();
        }
    }

    //endregion

    //region Container Implementation

    @Override
    public String getId() {
        return this.containerId;
    }

    //endregion

    //region Operations

    public CompletableFuture<Long> process(Operation operation) {
        Preconditions.checkState(this.operationQueue != null, "OperationProcessor is not running.");
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
     * @throws InterruptedException
     * @throws DataCorruptionException
     */

    private void runOnce() throws DataCorruptionException, InterruptedException {
        List<CompletableOperation> operations = this.operationQueue.takeAllEntries();
        log.debug("{}: RunOnce (OperationCount = {}).", this.traceObjectId, operations.size());
        if (operations.size() == 0) {
            // takeAllEntries() should have been blocking and not return unless it has data. If we get an empty response, just try again.
            return;
        }

        QueueProcessingState state = new QueueProcessingState(this.metadataUpdater, this.logUpdater, this.traceObjectId);
        try (DataFrameBuilder<Operation> dataFrameBuilder = new DataFrameBuilder<>(this.durableDataLog, state::commit, state::fail)) {
            for (CompletableOperation o : operations) {
                boolean processedSuccessfully = processOperation(o, dataFrameBuilder);

                // Add the operation as 'pending', only if we were able to successfully append it to a data frame.
                // We only commit data frames when we attempt to start a new record (if it's full) or if we try to close it, so we will not miss out on it.
                if (processedSuccessfully) {
                    state.addPending(o);
                }
            }
        }
        // Close the frame builder and ship any unsent frames.
        catch (Exception ex) {
            // Fail the current batch of operations with the given exception.
            state.fail(ex);

            // This is a nasty one. If we encountered a DataCorruptionException, it means we detected something abnormal
            // in our container. We need to shutdown right away.
            Throwable realCause = ExceptionHelpers.getRealException(ex);
            if (realCause instanceof DataCorruptionException) {
                throw (DataCorruptionException) realCause;
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
    private boolean processOperation(CompletableOperation operation, DataFrameBuilder<Operation> dataFrameBuilder) {
        Preconditions.checkState(!operation.isDone(), "The Operation has already been processed.");

        // Update Metadata and Operations with any missing data (offsets, lengths, etc) - the Metadata Updater has all the knowledge for that task.
        Operation entry = operation.getOperation();
        if (entry instanceof StorageOperation) {
            // We only need to update metadata for StorageOperations; MetadataOperations are internal and are processed by the requester.
            // We do this in two steps: first is pre-processing (updating the entry with offsets, lengths, etc.) and the second is acceptance.
            // Pre-processing does not have any effect on the metadata, but acceptance does.
            // That's why acceptance has to happen only after a successful append to the DataFrameBuilder.
            try {
                this.metadataUpdater.preProcessOperation((StorageOperation) entry);
            } catch (Exception ex) {
                // This entry was not accepted (due to external error) or some processing error occurred. Our only option is to fail it now, before trying to commit it.
                operation.fail(ex);
                return false;
            }
        }

        // Entry is ready to be serialized; assign a sequence number.
        entry.setSequenceNumber(this.metadataUpdater.getNewOperationSequenceNumber());

        log.trace("{}: DataFrameBuilder.Append {}.", this.traceObjectId, operation.getOperation());
        try {
            dataFrameBuilder.append(operation.getOperation());
        } catch (Exception ex) {
            operation.fail(ex);
            return false;
        }

        if (entry instanceof StorageOperation) {
            try {
                this.metadataUpdater.acceptOperation((StorageOperation) entry);
            } catch (MetadataUpdateException ex) {
                // This is an internal error. This shouldn't happen. The entry has been committed, but we couldn't update the metadata due to a bug.
                operation.fail(ex);
                return false;
            }
        }

        return true;
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

        public QueueProcessingState(OperationMetadataUpdater metadataUpdater, MemoryLogUpdater logUpdater, String traceObjectId) {
            assert metadataUpdater != null : "metadataUpdater is null";
            assert logUpdater != null : "logUpdater is null";

            this.traceObjectId = traceObjectId;
            this.pendingOperations = new LinkedList<>();
            this.metadataUpdater = metadataUpdater;
            this.logUpdater = logUpdater;
        }

        /**
         * Adds a new pending operation.
         *
         * @param operation The operation to append.
         */
        public void addPending(CompletableOperation operation) {
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

            // Commit any changes to metadata.
            this.metadataUpdater.commit();
            this.metadataUpdater.recordTruncationMarker(commitArgs.getLastStartedSequenceNumber(), commitArgs.getDataFrameSequence());

            // TODO: consider running this on its own thread, but they must still be in the same sequence!
            // Acknowledge all pending entries, in the order in which they are in the queue. It is important that we ack entries in order of increasing Sequence Number.
            while (this.pendingOperations.size() > 0 && this.pendingOperations.getFirst().getOperation().getSequenceNumber() <= commitArgs.getLastFullySerializedSequenceNumber()) {
                CompletableOperation e = this.pendingOperations.removeFirst();
                try {
                    logUpdater.add(e.getOperation());
                } catch (DataCorruptionException ex) {
                    log.error("{}: OperationCommitFailure ({}). {}", this.traceObjectId, e.getOperation(), ex);
                    e.fail(ex);
                    throw ex;
                }

                e.complete();
            }

            this.logUpdater.flush();
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
