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
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.MathHelpers;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.RuntimeStreamingException;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.Writer;
import com.emc.pravega.service.server.logs.operations.MetadataCheckpointOperation;
import com.emc.pravega.service.server.logs.operations.MetadataOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Storage Writer. Applies operations from Operation Log to Storage.
 */
@Slf4j
class StorageWriter extends AbstractService implements Writer {
    //region Members

    private final String traceObjectId;
    private final WriterConfig config;
    private final WriterDataSource dataSource;
    private final Storage storage;
    private final ScheduledExecutorService executor;
    private final HashMap<Long, SegmentAggregator> aggregators;
    private final AtomicReference<Throwable> stopException;
    private final WriterState state;
    private final AutoStopwatch stopwatch;
    private final AckCalculator ackCalculator;
    private final AtomicBoolean closed;
    private CompletableFuture<Void> runTask;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StorageWriter class.
     *
     * @param config     The WriterConfig to use.
     * @param dataSource The WriterDataSource to use.
     * @param storage    The Storage to use.
     * @param executor   The Executor to use for async callbacks and operations.
     */
    StorageWriter(WriterConfig config, WriterDataSource dataSource, Storage storage, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(dataSource, "dataSource");
        Preconditions.checkNotNull(storage, "storage");
        Preconditions.checkNotNull(executor, "executor");

        this.traceObjectId = String.format("StorageWriter[%d]", dataSource.getId());
        this.config = config;
        this.dataSource = dataSource;
        this.storage = storage;
        this.executor = executor;
        this.aggregators = new HashMap<>();
        this.state = new WriterState();
        this.stopwatch = new AutoStopwatch();
        this.stopException = new AtomicReference<>();
        this.ackCalculator = new AckCalculator(this.state);
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

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

    //region AbstractService Implementation

    @Override
    protected void doStart() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        notifyStarted();
        log.info("{}: Started.", this.traceObjectId);
        runContinuously();
    }

    @Override
    protected void doStop() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        log.info("{}: Stopping.", this.traceObjectId);

        this.executor.execute(() -> {
            Throwable cause = this.stopException.get();

            // Cancel the last iteration and wait for it to finish.
            if (this.runTask != null) {
                try {
                    // This doesn't actually cancel the task. We need to plumb through the code with 'checkRunning' to
                    // make sure we stop any long-running tasks.
                    this.runTask.get(this.config.getShutdownTimeout().toMillis(), TimeUnit.MILLISECONDS);
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

            log.info("{}: Stopped.", this.traceObjectId);
        });
    }

    //endregion

    // region Iteration Execution

    private void runContinuously() {
        // A Writer iteration is made of the following stages:
        // 1. Delay (if necessary).
        // 2. Read data.
        // 3. Load data into SegmentAggregators.
        // 4. Flush eligible SegmentAggregators.
        // 5. Acknowledge (truncate).
        this.runTask = FutureHelpers.loop(
                this::canRun,
                () -> FutureHelpers
                        .delayedFuture(getIterationStartDelay(), this.executor)
                        .thenAccept(this::beginIteration)
                        .thenCompose(this::readData)
                        .thenAcceptAsync(this::processReadResult, this.executor)
                        .thenCompose(this::flush)
                        .thenCompose(this::acknowledge)
                        .exceptionally(this::iterationErrorHandler)
                        .thenAccept(this::endIteration),
                this.executor);
    }

    private boolean canRun() {
        return isRunning() && this.stopException.get() == null;
    }

    private void beginIteration(Void ignored) {
        this.state.recordIterationStarted(this.stopwatch);
        logStageEvent("Start", null);
    }

    private void endIteration(Void ignored) {
        logStageEvent("Finish", "Elapsed " + this.state.getElapsedSinceIterationStart(this.stopwatch).toMillis() + "ms");
    }

    private Void iterationErrorHandler(Throwable ex) {
        boolean critical = isCriticalError(ex);
        if (!critical) {
            // Perform internal cleanup (get rid of those SegmentAggregators that are closed).
            cleanup();
        }

        if (ExceptionHelpers.getRealException(ex) instanceof CancellationException && !canRun()) {
            // Writer is not running and we caught a CancellationException.
            // This is a normal behavior and it is triggered by stopAsync(); just exit without logging or triggering anything else.
            log.info("{}: StorageWriter intercepted {} while shutting down.", this.traceObjectId, ExceptionHelpers.getRealException(ex).getClass().getSimpleName());
            return null;
        }

        logError(ex, critical);
        if (critical) {
            // Setting a stop exception guarantees the main Writer loop will not continue running again.
            this.stopException.set(ex);
            stopAsync();
        } else {
            this.state.recordIterationError();
        }

        return null;
    }

    //endregion

    //region Input Processing

    /**
     * Reads data from the OperationLog.
     *
     * @return A CompletableFuture that, when complete, will indicate that the read has been performed in its entirety.
     */
    private CompletableFuture<Iterator<Operation>> readData(Void ignored) {
        long traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "readData");
        try {
            Duration readTimeout = getReadTimeout();
            return this.dataSource
                    .read(this.state.getLastReadSequenceNumber(), this.config.getMaxItemsToReadAtOnce(), readTimeout)
                    .thenApply(result -> {
                        LoggerHelpers.traceLeave(log, this.traceObjectId, "readData", traceId);
                        return result;
                    })
                    .exceptionally(ex -> {
                        ex = ExceptionHelpers.getRealException(ex);
                        if (ex instanceof TimeoutException) {
                            // TimeoutExceptions are acceptable for Reads. In that case we just return null as opposed from
                            // killing the entire Iteration. Even if we were unable to read, we may still need to flush
                            // in this iteration or do other tasks.
                            log.warn("{}: Iteration[{}] No items were read during allotted timeout of {}ms", this.traceObjectId, this.state.getIterationId(), readTimeout.toMillis());
                            return null;
                        } else {
                            throw new CompletionException(ex);
                        }
                    });
        } catch (Throwable ex) {
            // This is for synchronous exceptions.
            Throwable realEx = ExceptionHelpers.getRealException(ex);
            if (realEx instanceof TimeoutException) {
                logErrorHandled(realEx);
                return CompletableFuture.completedFuture(null);
            } else {
                return FutureHelpers.failedFuture(ex);
            }
        }
    }

    /**
     * Processes all the operations in the given ReadResult.
     *
     * @param readResult The read result to process.
     */
    private void processReadResult(Iterator<Operation> readResult) {
        long traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "processReadResult");
        InputReadStageResult result = new InputReadStageResult(this.state);
        if (readResult == null) {
            // This happens when we get a TimeoutException from the read operation.
            logStageEvent("InputRead", result);
            LoggerHelpers.traceLeave(log, this.traceObjectId, "processReadResult", traceId);
            return;
        }

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
                    result.bytes += processStorageOperation((StorageOperation) op);
                } else {
                    // Unknown operation. Better throw an error rather than skipping over what could be important data.
                    throw new DataCorruptionException(String.format("Unsupported operation %s.", op));
                }

                // We have now internalized all operations from this batch; and even if subsequent operations in this iteration
                // fail, we no longer need to re-read these operations, so update the state with the last read SeqNo.
                this.state.setLastReadSequenceNumber(op.getSequenceNumber());
                result.count++;
            }
        } catch (DataCorruptionException ex) {
            throw new RuntimeStreamingException(ex);
        }

        logStageEvent("InputRead", result);
        LoggerHelpers.traceLeave(log, this.traceObjectId, "processReadResult", traceId);
    }

    private void processMetadataOperation(MetadataOperation op) throws DataCorruptionException {
        // We only care about MetadataCheckpointOperations; all others are no-ops here.
        if (op instanceof MetadataCheckpointOperation) {
            // We don't care about the contents of the operation, we just need to verify that it is correctly mapped to a Valid Truncation Point.
            if (!this.dataSource.isValidTruncationPoint(op.getSequenceNumber())) {
                throw new DataCorruptionException(String.format("Operation '%s' does not correspond to a valid Truncation Point in the metadata.", op));
            }
        }
    }

    private long processStorageOperation(StorageOperation op) throws DataCorruptionException {
        // Add the operation to the appropriate Aggregator.
        SegmentAggregator aggregator = getSegmentAggregator(op.getStreamSegmentId());
        aggregator.add(op);
        return op.getLength();
    }

    //endregion

    //region Stage Execution

    /**
     * Flushes eligible operations to Storage, if necessary. Does not perform any mergers.
     */
    private CompletableFuture<Void> flush(Void ignored) {
        checkRunning();
        long traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "flush");

        // Flush everything we can flush.
        val flushFutures = this.aggregators.values().stream()
                                           .filter(SegmentAggregator::mustFlush)
                                           .map(a -> a.flush(this.config.getFlushTimeout(), this.executor))
                                           .collect(Collectors.toList());

        return FutureHelpers
                .allOfWithResults(flushFutures)
                .thenAccept(flushResults -> {
                    FlushStageResult result = new FlushStageResult();
                    flushResults.forEach(result::withFlushResult);
                    if (result.getFlushedBytes() + result.getMergedBytes() + result.count > 0) {
                        logStageEvent("Flush", result);
                    }

                    LoggerHelpers.traceLeave(log, this.traceObjectId, "flush", traceId);
                });
    }

    /**
     * Cleans up all SegmentAggregators that are currently closed.
     */
    private void cleanup() {
        long traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "cleanup");
        val toRemove = this.aggregators.values().stream()
                                       .filter(SegmentAggregator::isClosed)
                                       .map(a -> a.getMetadata().getId())
                                       .collect(Collectors.toList());
        toRemove.forEach(this.aggregators::remove);
        LoggerHelpers.traceLeave(log, this.traceObjectId, "cleanup", traceId, toRemove.size());
    }

    /**
     * Acknowledges operations that were flushed to storage
     */
    private CompletableFuture<Void> acknowledge(Void ignored) {
        checkRunning();
        long traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "acknowledge");

        long highestCommittedSeqNo = this.ackCalculator.getHighestCommittedSequenceNumber(this.aggregators.values());
        long ackSequenceNumber = this.dataSource.getClosestValidTruncationPoint(highestCommittedSeqNo);
        if (ackSequenceNumber > this.state.getLastTruncatedSequenceNumber()) {
            // Issue the truncation and update the state (when done).
            return this.dataSource
                    .acknowledge(ackSequenceNumber, this.config.getAckTimeout())
                    .thenRun(() -> {
                        this.state.setLastTruncatedSequenceNumber(ackSequenceNumber);
                        logStageEvent("Acknowledged", "SeqNo=" + ackSequenceNumber);
                        LoggerHelpers.traceLeave(log, this.traceObjectId, "acknowledge", traceId, ackSequenceNumber);
                    });
        } else {
            // Nothing to do.
            LoggerHelpers.traceLeave(log, this.traceObjectId, "acknowledge", traceId, Operation.NO_SEQUENCE_NUMBER);
            return CompletableFuture.completedFuture(null);
        }
    }

    //endregion

    //region Helpers

    /**
     * Gets, or creates, a SegmentAggregator for the given StorageOperation.
     *
     * @param streamSegmentId The Id of the StreamSegment to get the aggregator for.
     * @throws DataCorruptionException If the Operation refers to a StreamSegmentId that does not exist in Metadata.
     */
    private SegmentAggregator getSegmentAggregator(long streamSegmentId) throws DataCorruptionException {
        SegmentAggregator result;
        result = this.aggregators.getOrDefault(streamSegmentId, null);
        if (result == null) {
            // We do not yet have this aggregator. First, get its metadata.
            UpdateableSegmentMetadata segmentMetadata = this.dataSource.getStreamSegmentMetadata(streamSegmentId);
            if (segmentMetadata == null) {
                throw new DataCorruptionException(String.format("No StreamSegment with id '%d' is registered in the metadata.", streamSegmentId));
            }

            // Then create the aggregator.
            result = new SegmentAggregator(segmentMetadata, this.dataSource, this.storage, this.config, this.stopwatch);
            this.aggregators.put(streamSegmentId, result);
            result.initialize(this.config.getFlushTimeout()).join();
        }

        return result;
    }

    private boolean isCriticalError(Throwable ex) {
        return ExceptionHelpers.mustRethrow(ex)
                || ExceptionHelpers.getRealException(ex) instanceof DataCorruptionException;
    }

    /**
     * Calculates the amount of time that should be used as a timeout for WriterDataSource reads. The following rules
     * are taken into consideration:
     * * If at least one SegmentAggregator needs to flush right away, the timeout returned is 0.
     * * The returned timeout is the amount of time until the first SegmentAggregator is due to flush.
     * * The returned timeout (except in the first case) is bounded by WriterConfig.MinReadTimeout and WriterConfig.MaxReadTimeout.
     */
    private Duration getReadTimeout() {
        // Find the minimum expiration time among all SegmentAggregators.
        long maxTimeMillis = this.config.getMaxReadTimeout().toMillis();
        long minTimeMillis = this.config.getMinReadTimeout().toMillis();
        long timeMillis = maxTimeMillis;
        for (SegmentAggregator a : this.aggregators.values()) {
            if (a.mustFlush()) {
                // We found a SegmentAggregator that needs to flush right away. No need to search anymore.
                timeMillis = 0;
                break;
            }

            timeMillis = MathHelpers.minMax(this.config.getFlushThresholdTime().minus(a.getElapsedSinceLastFlush()).toMillis(), minTimeMillis, timeMillis);
        }

        return Duration.ofMillis(timeMillis);
    }

    /**
     * Calculates the amount of delay for an iteration start, based on whether the previous iteration resulted in an error or not.
     */
    private Duration getIterationStartDelay() {
        if (this.state.getLastIterationError()) {
            return this.config.getErrorSleepDuration();
        } else {
            // No error, we can proceed right away.
            return Duration.ZERO;
        }
    }

    private void logStageEvent(String stageName, Object result) {
        if (result == null) {
            log.debug("{}: Iteration[{}].{}.", this.traceObjectId, this.state.getIterationId(), stageName);
        } else {
            log.debug("{}: Iteration[{}].{} ({}).", this.traceObjectId, this.state.getIterationId(), stageName, result);
        }
        //System.out.println(String.format("%s: Iteration[%s].%s (%s).", this.traceObjectId, this.state.getIterationId(), stageName, result));
    }

    private void logError(Throwable ex, boolean critical) {
        ex = ExceptionHelpers.getRealException(ex);
        if (critical) {
            log.error("{}: Iteration[{}].CriticalError. {}", this.traceObjectId, this.state.getIterationId(), ex);
        } else {
            log.error("{}: Iteration[{}].Error. {}", this.traceObjectId, this.state.getIterationId(), ex);
        }
        //System.out.println(String.format("%s: Iteration[%s].Error. %s", this.traceObjectId, this.state.getIterationId(), ex));
    }

    private void logErrorHandled(Throwable ex) {
        ex = ExceptionHelpers.getRealException(ex);
        log.warn("{}: Iteration[{}].HandledError {}", this.traceObjectId, this.state.getIterationId(), ex);
        //        System.out.println(String.format("%s: Iteration[%s].Warn. %s", this.traceObjectId, this.state.getIterationId(), ex));
    }

    private void checkRunning() {
        if (!canRun()) {
            throw new CancellationException("StorageWriter has been stopped.");
        }
    }

    //endregion

    //region FlushStageResult

    /**
     * Represents the result of an iteration stage.
     */
    private static class FlushStageResult extends FlushResult {
        int count;

        @Override
        public FlushStageResult withFlushResult(FlushResult flushResult) {
            this.count++;
            return (FlushStageResult) super.withFlushResult(flushResult);
        }

        @Override
        public String toString() {
            return String.format("Count=%d, %s", this.count, super.toString());
        }
    }

    /**
     * Represents the result of the Read stage.
     */
    private static class InputReadStageResult {
        int count;
        long bytes;
        private final WriterState state;

        InputReadStageResult(WriterState state) {
            this.state = state;
        }

        @Override
        public String toString() {
            return String.format("Count=%d, Bytes=%d, LastReadSN=%d", this.count, this.bytes, this.state.getLastReadSequenceNumber());
        }
    }

    //endregion
}
