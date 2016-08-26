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

import com.emc.pravega.common.AutoStopwatch;
import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.RuntimeStreamingException;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.logs.OperationLog;
import com.emc.pravega.service.server.logs.operations.MergeBatchOperation;
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
    private static final Duration FLUSH_TIMEOUT = GENERAL_TIMEOUT;
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
    private final WriterState state;
    private final AutoStopwatch stopwatch;
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
        this.state = new WriterState();
        this.stopwatch = new AutoStopwatch();
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
        });
    }

    @Override
    protected void doStop() {
        Exceptions.checkNotClosed(this.closed, this);
        log.info("{} Stopping ...", this.traceObjectId);

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
     * @param ignored Not used.
     * @param ex      (Optional) An exception that was thrown during the execution of the iteration.
     */
    private void endOfIteration(Void ignored, Throwable ex) {
        assert this.currentIteration != null : "No iteration is in progress";
        this.currentIteration = null;
        if (ex != null) {
            if (ExceptionHelpers.getRealException(ex) instanceof CancellationException && !isRunning()) {
                // Writer is not running and we caught a CancellationException.
                // This is a normal behavior and it is triggered by stopAsync(); just exit without logging or triggering anything else.
                return;
            }

            System.err.println("Iteration[" + this.iterationId + "].Error: " + ex);
            ExceptionHelpers.getRealException(ex).printStackTrace(System.err);
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
    private CompletableFuture<Iterator<Operation>> readData() {
        Duration readTimeout = getReadTimeout();
        return this.operationLog.read(this.state.getLastReadSequenceNumber(), this.config.getMaxItemsToReadAtOnce(), readTimeout);
    }

    /**
     * Processes all the operations in the given ReadResult.
     *
     * @param readResult The read result to process.
     */
    private void processReadResult(Iterator<Operation> readResult) {
        int count = 0;
        try {
            while (readResult.hasNext()) {
                checkRunning();
                Operation op = readResult.next();

                // Verify that the Operation we got is in the correct order (check Sequence Number).
                if (op.getSequenceNumber() <= this.state.getLastReadSequenceNumber()) {
                    throw new DataCorruptionException(String.format("Operation '%s' has a sequence number that is lower than the previous one (%d).", op, this.state.getLastReadSequenceNumber()));
                }

                if (op instanceof MetadataOperation) {
                    processMetadataOperation((MetadataOperation) op);
                } else if (op instanceof StorageOperation) {
                    processStorageOperation((StorageOperation) op);
                } else {
                    // Unknown operation. Better throw an error rather than skipping over what could be important data.
                    throw new DataCorruptionException(String.format("Unsupported operation %s.", op));
                }

                // We have now internalized all operations from this batch; and even if subsequent operations in this iteration
                // fail, we no longer need to re-read these operations, so update the state with the last read SeqNo.
                this.state.setLastReadSequenceNumber(op.getSequenceNumber());
                count++;
            }
        } catch (DataCorruptionException ex) {
            throw new RuntimeStreamingException(ex);
        }

        log.info("{}: Iteration[{}].ReadResult (Count = {}, LastSN = {})", this.traceObjectId, this.iterationId, count, this.state.getLastReadSequenceNumber());
        System.out.println(String.format("Iteration[%d].ReadResult (Count = %d, LastSN = %d)", this.iterationId, count, this.state.getLastReadSequenceNumber()));
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
        // Add the operation to the appropriate Aggregator.
        SegmentAggregator aggregator = getSegmentAggregator(op.getStreamSegmentId());
        aggregator.add(op);
        if (op instanceof MergeBatchOperation) {
            // If a Batch, it needs to be added both to the Parent StreamSegment and to the batch StreamSegment.
            aggregator = getSegmentAggregator(((MergeBatchOperation) op).getBatchStreamSegmentId());
            aggregator.add(op);
        }
    }

    //endregion

    /**
     * Flushes eligible operations to Storage, if necessary.
     *
     * @return A CompletableFuture with the result, when the operation completes.
     */
    private CompletableFuture<FlushResult> flush() {
        checkRunning();

        ArrayList<CompletableFuture<FlushResult>> flushFutures = new ArrayList<>();
        for (SegmentAggregator a : this.aggregators.values()) {
            if (a.mustFlush()) {
                // We have a Segment that is ripe for flushing.
                flushFutures.add(a.flush(this.storage, FLUSH_TIMEOUT));
            }

            // TODO: think how to do merges properly. Care must be taken to properly sync the parents and their batches and to make sure one doesn't block the other too much.
        }

        FutureHelpers.allOfWithResults(flushFutures);

        //
        return CompletableFuture.completedFuture(null);
    }

    private void merge() {

    }

    /**
     * Acknowledges operations that were flushed to storage
     *
     * @param flushResult The FlushResult returned from the flush() method.
     */
    private void acknowledge(FlushResult flushResult) {
        checkRunning();
        // The Sequence Number we acknowledge has the property that all operations up to, and including it, have been
        // committed to Storage.

        // This can only be calculated by looking at all the active SegmentAggregators and picking the smallest

        long lowestUncommittedSeqNo = this.state.getLastReadSequenceNumber();
        for (SegmentAggregator a : this.aggregators.values()) {
            long firstSeqNo = a.getLowestUncommittedSequenceNumber();
            if (firstSeqNo >= 0) {
                lowestUncommittedSeqNo = Math.min(lowestUncommittedSeqNo, firstSeqNo);
            }
        }

        this.state.setLowestUncommitedSequenceNumber(lowestUncommittedSeqNo);

        // TODO: issue truncate
    }

    /**
     * Gets, or creates, a SegmentAggregator for the given StorageOperation.
     *
     * @param streamSegmentId The Id of the StreamSegment to get the aggregator for.
     * @return The result.
     * @throws DataCorruptionException If the Operation refers to a StreamSegmentId that does not exist in Metadata.
     */
    private SegmentAggregator getSegmentAggregator(long streamSegmentId) throws DataCorruptionException {
        SegmentAggregator result;
        boolean needsInitialization = false;
        synchronized (this.aggregators) {
            result = this.aggregators.getOrDefault(streamSegmentId, null);
            if (result == null) {
                // We do not yet have this aggregator. First, get its metadata.
                UpdateableSegmentMetadata segmentMetadata = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
                if (segmentMetadata == null) {
                    throw new DataCorruptionException(String.format("No StreamSegment with id '%d' is registered in the metadata.", streamSegmentId));
                }

                // Then create the aggregator.
                result = new SegmentAggregator(segmentMetadata, this.config, this.stopwatch);
                this.aggregators.put(streamSegmentId, result);
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
}
