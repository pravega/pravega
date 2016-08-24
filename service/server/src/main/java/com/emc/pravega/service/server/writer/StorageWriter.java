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

package com.emc.pravega.service.server.writer;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.contracts.RuntimeStreamingException;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.logs.OperationLog;
import com.emc.pravega.service.server.logs.operations.MetadataCheckpointOperation;
import com.emc.pravega.service.server.logs.operations.MetadataOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Storage Writer. Applies operations from Operation Log to Storage.
 */
@Slf4j
class StorageWriter extends AbstractService implements Writer {
    //region Members

    private static final Duration GENERAL_TIMEOUT = Duration.ofSeconds(30); //TODO: break down into specific timeouts, or something smarter.
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private final String traceObjectId;
    private final WriterConfig config;
    private final UpdateableContainerMetadata containerMetadata;
    private final OperationLog operationLog;
    private final Storage storage;
    private final Cache cache;
    private final Executor executor;
    private final HashMap<Long, SegmentAggregator> aggregators;
    private final AtomicReference<Throwable> stopException = new AtomicReference<>();
    private final State state;
    private CompletableFuture<Void> currentIteration;
    private long iterationId;
    private boolean closed;

    //endregion

    //region Constructor

    StorageWriter(WriterConfig config, UpdateableContainerMetadata containerMetadata, OperationLog operationLog, Storage storage, Cache cache, Executor executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(containerMetadata, "containerMetadata");
        Preconditions.checkNotNull(operationLog, "operationLog");
        Preconditions.checkNotNull(storage, "storage");
        Preconditions.checkNotNull(cache, "cache");
        Preconditions.checkNotNull(executor, "executor");

        this.traceObjectId = String.format("StorageWriter[%d]", containerMetadata.getContainerId());
        this.config = config;
        this.containerMetadata = containerMetadata;
        this.operationLog = operationLog;
        this.storage = storage;
        this.cache = cache;
        this.executor = executor;
        this.aggregators = new HashMap<>();
        this.state = new State();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            stopAsync();
            ServiceShutdownListener.awaitShutdown(this, false);

            log.info("{} Closed.", this.traceObjectId);
            this.closed = true;
        }
    }

    //endregion

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        Exceptions.checkNotClosed(this.closed, this);
        this.executor.execute(() -> {
            runOneIteration();
            notifyStarted();
            log.info("{} Started.", this.traceObjectId);
            System.out.println(this.traceObjectId + " Started");
        });
    }

    @Override
    protected void doStop() {
        Exceptions.checkNotClosed(this.closed, this);
        log.info("{} Stopping ...", this.traceObjectId);
        System.out.println(this.traceObjectId + " Stopping ...");

        this.executor.execute(() -> {
            Throwable cause = this.stopException.get();

            // Cancel the last iteration and wait for it to finish.
            CompletableFuture<Void> lastIteration = this.currentIteration;
            if (lastIteration != null) {
                try {
                    // This doesn't actually cancel the task. We need to plumb through the code with 'checkRunning' to
                    // make sure we stop any long-running tasks.
                    lastIteration.cancel(true);
                    lastIteration.get(SHUTDOWN_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                } catch (Exception ex) {
                    if (cause != null) {
                        cause = ex;
                    }
                }
            }

            if (cause == null) {
                // Normal shutdown.
                notifyStopped();
            } else {
                // Shutdown caused by some failure.
                notifyFailed(cause);
            }

            log.info("{} Stopped.", this.traceObjectId);
            System.out.println(this.traceObjectId + " Stopped.");
        });
    }

    //endregion

    // region Iteration Execution

    /**
     * Starts the execution of one iteration.
     */
    private void runOneIteration() {
        assert this.currentIteration == null : "Another iteration is in progress";
        this.iterationId++;
        log.debug("{}: Iteration[{}].Start.", this.traceObjectId, this.iterationId);

        this.currentIteration = readData()
                .thenAcceptAsync(this::processReadResult, this.executor)
                .thenComposeAsync(v -> this.flush(), this.executor)
                .thenAcceptAsync(this::acknowledge, this.executor)
                .whenCompleteAsync(this::endOfIteration, this.executor);
    }

    /**
     * Called when an iteration is complete, whether successfully or not.
     *
     * @param result
     * @param ex
     */
    private void endOfIteration(Void result, Throwable ex) {
        assert this.currentIteration != null : "No iteration is in progress";
        this.currentIteration = null;
        if (ex != null) {
            if (ExceptionHelpers.getRealException(ex) instanceof CancellationException && !isRunning()) {
                // Writer is not running and we caught a CancellationException.
                // This is a normal behavior and it is triggered by stopAsync(); just exit without logging or triggering anything else.
                return;
            }

            System.err.println("Iteration[" + this.iterationId + "].Error: " + ex);
            log.error("{}: Iteration[{}].Error. {}", this.traceObjectId, this.iterationId, ex);
            if (isCriticalError(ex)) {
                this.stopException.set(ex);
                stopAsync();
                return;
            }
        }

        log.debug("{}: Iteration[{}].Finish.", this.traceObjectId, this.iterationId);
        if (isRunning()) {
            runOneIteration();
        }
    }

    //endregion

    //region Input Processing

    /**
     * Reads data from the OperationLog.
     *
     * @return A CompletableFuture that, when complete, will indicate that the read has been performed in its entirety.
     */
    private CompletableFuture<OperationReadResult> readData() {
        // Calculate the timeout for the operation.
        Duration readTimeout = getReadTimeout();

        // Initiate a Read from the OperationLog, then load it up into a concrete object, and return that.
        return this.operationLog
                .read(this.state.getLastReadSequenceNumber(), this.config.getMaxItemsToReadAtOnce(), readTimeout)
                .thenApplyAsync(this::processReadResult, this.executor);
    }

    /**
     * Loads up all the Operations from the given iterator (readResult) into an OperationReadResult.
     *
     * @param readResult The readResult iterator to read from.
     * @return The result.
     */
    private OperationReadResult processReadResult(Iterator<Operation> readResult) {
        OperationReadResult result = new OperationReadResult(this.state);
        try {
            while (readResult.hasNext()) {
                checkRunning();
                result.include(readResult.next());
            }
        } catch (DataCorruptionException ex) {
            throw new RuntimeStreamingException(ex);
        }

        return result;
    }

    /**
     * Processes all the operations in the given OperationReadResult.
     *
     * @param readResult
     */
    private void processReadResult(OperationReadResult readResult) {
        log.info("{}: Iteration[{}].ReadResult ({})", this.traceObjectId, this.iterationId, readResult);
        System.out.println(String.format("Iteration[%d].ReadResult: %s", this.iterationId, readResult));

        if (readResult.getItems().size() > 0) {
            try {
                for (Operation op : readResult.getItems()) {
                    checkRunning();
                    if (op instanceof MetadataOperation) {
                        processMetadataOperation((MetadataOperation) op);
                    } else if (op instanceof StorageOperation) {
                        processStorageOperation((StorageOperation) op);
                    } else {
                        throw new DataCorruptionException(String.format("Unsupported operation %s.", op));
                    }
                }
            } catch (DataCorruptionException ex) {
                throw new RuntimeStreamingException(ex);
            }

            // We have now internalized all operations from this batch; and even if subsequent operations in this iteration
            // fail, we no longer need to re-read these operations, so update the state with the last read SeqNo.
            this.state.setLastReadSequenceNumber(readResult.getLastSequenceNumber());
        }
    }

    private void processMetadataOperation(MetadataOperation op) throws DataCorruptionException {
        // We only care about MetadataCheckpointOperations; all others are no-ops here.
        if (op instanceof MetadataCheckpointOperation) {
            // We don't care about the contents of the operation, we just need to verify that it is correctly mapped to a Valid Truncation Point.
            if (!this.containerMetadata.isValidTruncationPoint(op.getSequenceNumber())) {
                throw new DataCorruptionException(String.format("Operation '%s' does not correspond to a valid Truncation Point in the metadata.", op));
            }
        }
    }

    private void processStorageOperation(StorageOperation op) throws DataCorruptionException {
        SegmentAggregator aggregator = getSegmentAggregator(op);
        // TODO: finish this: add validation (here or in the aggregator, etc).
    }

    //endregion

    /**
     * Flushes eligible operations to Storage, if necessary.
     *
     * @return A CompletableFuture with the result, when the operation completes.
     */
    private CompletableFuture<FlushResult> flush() {
        checkRunning();
        return null;
    }

    private void merge() {

    }

    /**
     * Acknowledges operations that were flushed to storage
     *
     * @param flushResult
     */
    private void acknowledge(FlushResult flushResult) {
        checkRunning();
    }

    /**
     * Gets, or creates, a SegmentAggregator for the given StorageOperation.
     *
     * @param operation The Operation to get the Aggregator for.
     * @return The result.
     * @throws DataCorruptionException If the Operation refers to a StreamSegmentId that does not exist in Metadata.
     */
    private SegmentAggregator getSegmentAggregator(StorageOperation operation) throws DataCorruptionException {
        SegmentAggregator result;
        boolean needsInitialization = false;
        synchronized (this.aggregators) {
            result = this.aggregators.getOrDefault(operation.getStreamSegmentId(), null);
            if (result == null) {
                // We do not yet have this aggregator. First, get its metadata.
                UpdateableSegmentMetadata sm = this.containerMetadata.getStreamSegmentMetadata(operation.getStreamSegmentId());
                if (sm == null) {
                    throw new DataCorruptionException(String.format("Operation '%s' refers to a StreamSegment that is not registered in the metadata.", operation));
                }

                // Then create the aggregator.
                result = new SegmentAggregator(sm);
                this.aggregators.put(operation.getStreamSegmentId(), result);
                needsInitialization = true;
            }
        }

        if (needsInitialization) {
            result.initialize(this.storage, GENERAL_TIMEOUT).join();
        }

        return result;
    }

    private boolean isCriticalError(Throwable ex) {
        return ExceptionHelpers.mustRethrow(ex)
                || ExceptionHelpers.getRealException(ex) instanceof DataCorruptionException;
    }

    private Duration getReadTimeout() {
        //TODO: calculate the time until the first Aggregated Buffer expires (needs to flush), or, if no such thing, the default value (30 mins).
        return this.config.getFlushThresholdTime();
    }

    private void checkRunning() {
        if (!isRunning()) {
            throw new CancellationException("StorageWriter has been stopped.");
        }
    }

    //region OperationReadResult

    private static class OperationReadResult {
        private final ArrayList<Operation> operations;
        private long firstSequenceNumber;
        private long lastSequenceNumber;

        OperationReadResult(State currentState) {
            this.operations = new ArrayList<>();
            this.firstSequenceNumber = currentState.getLastReadSequenceNumber();
            this.lastSequenceNumber = currentState.getLastReadSequenceNumber();
        }

        void include(Operation operation) throws DataCorruptionException {
            if (operation.getSequenceNumber() <= this.lastSequenceNumber) {
                throw new DataCorruptionException(String.format("Operation '%s' has a sequence number that is lower than the previous one (%d).", operation, this.lastSequenceNumber));
            }

            if (this.operations.size() == 0) {
                this.firstSequenceNumber = operation.getSequenceNumber();
            }

            this.operations.add(operation);
            this.lastSequenceNumber = operation.getSequenceNumber();
        }

        long getFirstSequenceNumber() {
            return this.firstSequenceNumber;
        }

        long getLastSequenceNumber() {
            return this.lastSequenceNumber;
        }

        public List<Operation> getItems() {
            return this.operations;
        }

        @Override
        public String toString() {
            if (this.operations.size() == 0) {
                return "Count = 0";
            } else {
                return String.format("Count = %d, FirstSN = %d, LastSN = %d", this.operations.size(), this.firstSequenceNumber, this.lastSequenceNumber);
            }
        }
    }

    //endregion

    //region State

    private static class State {
        private long lastReadSequenceNumber;
        private long highestCommittedSequenceNumber;

        State() {
            this.lastReadSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
            this.highestCommittedSequenceNumber = Operation.NO_SEQUENCE_NUMBER;
        }

        /**
         * Gets a value indicating the Sequence Number of the last read Operation (from the Operation Log).
         *
         * @return The result.
         */
        long getLastReadSequenceNumber() {
            return this.lastReadSequenceNumber;
        }

        /**
         * Sets the Sequence Number of the last read Operation.
         *
         * @param value The Sequence Number to set.
         */
        void setLastReadSequenceNumber(long value) {
            Preconditions.checkArgument(value >= this.lastReadSequenceNumber, "New LastReadSequenceNumber cannot be smaller than the previous one.");
            this.lastReadSequenceNumber = value;
        }

        /**
         * Gets a value indicating the Sequence Number of the last Operation that was committed to Storage, having the
         * property that all Operations prior to it have also been successfully committed.
         *
         * @return The result.
         */
        long getHighestCommittedSequenceNumber() {
            return this.highestCommittedSequenceNumber;
        }

        /**
         * Sets the Sequence Number of the last Operation committed to Storage (with all prior Operations also committed).
         *
         * @param value The Sequence Number to set.
         */
        void setHighestCommittedSequenceNumber(long value) {
            Preconditions.checkArgument(value >= this.highestCommittedSequenceNumber, "New highestCommittedSequenceNumber cannot be smaller than the previous one.");
            Preconditions.checkArgument(value <= this.lastReadSequenceNumber, "New highestCommittedSequenceNumber cannot be larger than lastReadSequenceNumber.");
            this.highestCommittedSequenceNumber = value;
        }

        @Override
        public String toString() {
            return String.format("LastRead = %d, HighestCommitted = %d", this.lastReadSequenceNumber, this.highestCommittedSequenceNumber);
        }
    }

    //endregion
}
