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
package io.pravega.segmentstore.server.writer;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.MathHelpers;
import io.pravega.common.ObjectClosedException;
import io.pravega.common.Timer;
import io.pravega.common.concurrent.AbstractThreadPoolService;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.concurrent.SequentialProcessor;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.segmentstore.server.ServiceHaltException;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.Writer;
import io.pravega.segmentstore.server.WriterFactory;
import io.pravega.segmentstore.server.WriterFlushResult;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.segmentstore.server.logs.operations.MetadataOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.storage.DataLogWriterNotPrimaryException;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageNotPrimaryException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Storage Writer. Applies operations from Operation Log to Storage.
 */
@Slf4j
class StorageWriter extends AbstractThreadPoolService implements Writer {
    //region Members

    private final WriterConfig config;
    private final WriterDataSource dataSource;
    private final Storage storage;
    private final HashMap<Long, ProcessorCollection> processors;
    private final WriterState state;
    private final Timer timer;
    private final AckCalculator ackCalculator;
    private final WriterFactory.CreateProcessors createProcessors;
    private final SequentialProcessor ackProcessor;
    private final SegmentStoreMetrics.StorageWriter metrics;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StorageWriter class.
     *
     * @param config           The WriterConfig to use.
     * @param dataSource       The WriterDataSource to use.
     * @param storage          The Storage to use.
     * @param createProcessors A Function, that, when invoked with a Segment Metadata as an argument, will return a Collection
     *                         of WriterSegmentProcessors to handle that Segment's operations.
     * @param executor         The Executor to use for async callbacks and operations.
     */
    StorageWriter(WriterConfig config, WriterDataSource dataSource, Storage storage, WriterFactory.CreateProcessors createProcessors,
                  ScheduledExecutorService executor) {
        super(String.format("StorageWriter[%d]", dataSource.getId()), executor);

        // No need to check dataSource or executor != null as the super() call above takes care of that.
        this.config = Preconditions.checkNotNull(config, "config");
        this.dataSource = dataSource;
        this.storage = Preconditions.checkNotNull(storage, "storage");
        this.createProcessors = Preconditions.checkNotNull(createProcessors, "createProcessors");
        this.processors = new HashMap<>();
        this.state = new WriterState();
        this.timer = new Timer();
        this.ackCalculator = new AckCalculator(this.state);
        this.ackProcessor = new SequentialProcessor(this.executor);
        this.metrics = new SegmentStoreMetrics.StorageWriter(dataSource.getId());
    }

    //endregion

    //region AbstractThreadPoolService Implementation

    @Override
    protected Duration getShutdownTimeout() {
        return this.config.getShutdownTimeout();
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        // A Writer iteration is made of the following stages:
        // 1. Delay (if necessary).
        // 2. Read data.
        // 3. Load data into SegmentProcessors.
        // 4. Flush eligible SegmentProcessors.
        // 5. Acknowledge (truncate).
        return Futures.loop(
                this::canRun,
                () -> Futures
                        .delayedFuture(getIterationStartDelay(), this.executor)
                        .thenRun(this::beginIteration)
                        .thenComposeAsync(this::readData, this.executor)
                        .thenComposeAsync(this::processReadResult, this.executor)
                        .thenComposeAsync(this::flush, this.executor)
                        .thenRunAsync(this::triggerAcknowledge, this.executor)
                        .exceptionally(this::iterationErrorHandler)
                        .thenRunAsync(this::endIteration, this.executor),
                this.executor)
                      .thenRun(this::closeProcessors);
    }

    private boolean canRun() {
        return isRunning() && getStopException() == null;
    }

    private void beginIteration() {
        this.state.recordIterationStarted(this.timer);
        logStageEvent("Start", null);
    }

    private void endIteration() {
        // Perform internal cleanup (get rid of those SegmentProcessors that are closed).
        cleanup();
        Duration elapsed = this.state.getElapsedSinceIterationStart(this.timer);
        this.metrics.iterationComplete(elapsed);
        logStageEvent("Finish", "Elapsed " + elapsed.toMillis() + "ms");
    }

    private Void iterationErrorHandler(Throwable ex) {
        if (isShutdownException(ex) && !canRun()) {
            // Writer is not running and we caught a CancellationException.
            // This is a normal behavior and it is triggered by stopAsync(); just exit without logging or triggering anything else.
            log.info("{}: StorageWriter intercepted {} while shutting down.", this.traceObjectId, Exceptions.unwrap(ex).getClass().getSimpleName());
            return null;
        }

        boolean critical = isCriticalError(ex);
        logError(ex, critical);
        if (critical) {
            // Setting a stop exception guarantees the main Writer loop will not continue running again.
            super.errorHandler(ex);
            stopAsync();
        } else {
            this.state.recordIterationError();
        }

        return null;
    }

    /**
     * Closes all processors. This is usually done when the StorageWriter has stopped or is about to stop.
     */
    private void closeProcessors() {
        this.processors.values().forEach(ProcessorCollection::close);
        this.processors.clear();
        this.ackProcessor.close();
        this.metrics.close();
    }

    //endregion

    //region Input Processing

    /**
     * Reads data from the OperationLog.
     *
     * @return A CompletableFuture that, when complete, will indicate that the read has been performed in its entirety.
     */
    private CompletableFuture<Queue<Operation>> readData(Void ignored) {
        val lastRead = this.state.getLastRead();
        if (lastRead != null && !lastRead.isEmpty()) {
            // This happens if, during a previous iteration, we had an unexpected error preventing processReadResult from
            // execution to completion (i.e., SegmentAggregator.initialize() failed). In that case, it is imperative that
            // we do not miss out on operations, otherwise that can translate to either a blocking error (no progress can
            // be made) or a data loss.
            log.info("{}: Iteration[{}] Not performing a new read because there are still {} items unprocessed from the previous iteration.",
                    this.traceObjectId, this.state.getIterationId(), lastRead.size());
            return CompletableFuture.completedFuture(lastRead);
        }

        try {
            Duration readTimeout = getReadTimeout();
            return this.dataSource
                    .read(this.config.getMaxItemsToReadAtOnce(), readTimeout)
                    .thenApply(this.state::setLastRead)
                    .exceptionally(ex -> {
                        ex = Exceptions.unwrap(ex);
                        if (ex instanceof TimeoutException) {
                            // TimeoutExceptions are acceptable for Reads. In that case we just return null as opposed from
                            // killing the entire Iteration. Even if we were unable to read, we may still need to flush
                            // in this iteration or do other tasks.
                            log.debug("{}: Iteration[{}] No items were read during allotted timeout of {}ms", this.traceObjectId, this.state.getIterationId(), readTimeout.toMillis());
                            return null;
                        } else {
                            throw new CompletionException(ex);
                        }
                    });
        } catch (Throwable ex) {
            // This is for synchronous exceptions.
            Throwable realEx = Exceptions.unwrap(ex);
            if (realEx instanceof TimeoutException) {
                logErrorHandled(realEx);
                return CompletableFuture.completedFuture(null);
            } else {
                return Futures.failedFuture(ex);
            }
        }
    }

    /**
     * Processes all the operations in the given ReadResult.
     *
     * @param readResult The read result to process.
     */
    private CompletableFuture<Void> processReadResult(Queue<Operation> readResult) {
        InputReadStageResult result = new InputReadStageResult(this.state);
        if (readResult == null) {
            // This happens when we get a TimeoutException from the read operation.
            this.state.recordReadComplete();
            this.metrics.readComplete(0);
            logStageEvent("InputRead", result);
            return CompletableFuture.completedFuture(null);
        }

        return Futures.loop(
                () -> canRun() && !readResult.isEmpty(),
                () -> {
                    // Peek, but do not remove, the first operation. We want to ensure that we have properly processed it
                    // so that we don't lose track of it.
                    Operation op = readResult.peek();
                    return processOperation(op).thenRun(() -> {
                        // We have now internalized all operations from this batch; and even if subsequent operations in this iteration
                        // fail, we no longer need to re-read these operations, so update the state with the last read SeqNo.
                        this.state.setLastReadSequenceNumber(op.getSequenceNumber());
                        readResult.poll(); // Actually remove the operation now.
                        result.operationProcessed(op);
                    });
                },
                this.executor)
                .thenRun(() -> {
                    // Clear the last read result from the state before exiting.
                    Preconditions.checkState(readResult.isEmpty(), "processReadResult exited normally but there are still {} items to process.", readResult.size());
                    this.state.setLastRead(null);
                    logStageEvent("InputRead", result);
                });
    }

    private CompletableFuture<Void> processOperation(Operation op) {
        // Verify that the Operation we got is in the correct order (check Sequence Number).
        if (op.getSequenceNumber() <= this.state.getLastReadSequenceNumber()) {
            return Futures.failedFuture(new DataCorruptionException(String.format(
                    "Operation '%s' has a sequence number that is lower than the previous one (%d).", op, this.state.getLastReadSequenceNumber())));
        }

        if (op instanceof SegmentOperation) {
            return processSegmentOperation((SegmentOperation) op);
        } else if (op instanceof MetadataOperation) {
            return processMetadataOperation((MetadataOperation) op);
        } else {
            // Unknown operation. Better throw an error rather than skipping over what could be important data.
            return Futures.failedFuture(new ServiceHaltException(String.format("Unsupported operation %s.", op)));
        }
    }

    private CompletableFuture<Void> processMetadataOperation(MetadataOperation op) {
        // We only care about MetadataCheckpointOperations; all others are no-ops here.
        if (op instanceof MetadataCheckpointOperation) {
            // We don't care about the contents of the operation, we just need to verify that it is correctly mapped to a Valid Truncation Point.
            if (!this.dataSource.isValidTruncationPoint(op.getSequenceNumber())) {
                return Futures.failedFuture(new DataCorruptionException(String.format(
                        "Operation '%s' does not correspond to a valid Truncation Point in the metadata.", op)));
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> processSegmentOperation(SegmentOperation op) {
        val segmentMetadata = this.dataSource.getStreamSegmentMetadata(op.getStreamSegmentId());
        if (segmentMetadata == null) {
            // We have seen instances post "container recoveries" where we get segmentMetadata as null here which ideally shouldnt happen.
            // SegmentMetadata being null here after it has been through the OperationProcessor basically
            // means the segment metadata is removed from the containerMetadata( StorageWriters' WriterDataSource.metadata).
            // Segment metadata being removed can (only) happen if the metadata was actually evicted and not present in MetadataCheckpoints
            // after this `op` was processed.
            // Segment evicted means the effects of metadata have already been recorded through StorageWriter for this op.
            // Yet if we still land here, just ignore these operations instead of failing the pipeline later with DataCorruption.
            log.warn("{}: No segment metadata found. Ignoring this operation {}", this.traceObjectId, op);
            return CompletableFuture.completedFuture(null);
        }

        // Add the operation to the appropriate Aggregator.
        return getProcessor(op.getStreamSegmentId())
                .thenAccept(aggregator -> {
                    try {
                        aggregator.add(op);
                    } catch (ServiceHaltException ex) {
                        // Covers DataCorruptionException
                        // Re-throw.
                        throw new CompletionException(ex);
                    } catch (Exception ex) {
                        // If we get any exception while processing this, then we will likely get it every time we attempt
                        // to do it again. Best if we bail out and attempt a recovery in this case.
                        throw new CompletionException(new ServiceHaltException("Unable to process operation " + op, ex));
                    }
                });
    }

    //endregion

    //region Stage Execution

    @Override
    public CompletableFuture<Boolean> forceFlush(long upToSequenceNumber, Duration timeout) {
        // Wait for current iteration to complete; next iteration will be a force flush. (Set a flag or something).
        return this.state.setForceFlush(upToSequenceNumber);
    }

    /**
     * Flushes eligible operations to Storage, if necessary. Does not perform any mergers.
     */
    private CompletableFuture<Void> flush(Void ignored) {
        checkRunning();
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "flush");

        // Flush everything we can flush.
        val timer = new Timer();
        val forceFlush = this.state.isForceFlush();
        val flushFutures = this.processors.values().stream()
                .filter(pc -> forceFlush || pc.mustFlush())
                .map(a -> a.flush(forceFlush, this.config.getFlushTimeout()))
                .collect(Collectors.toList());

        return Futures
                .allOfWithResults(flushFutures)
                .thenAcceptAsync(flushResults -> {
                    FlushStageResult result = new FlushStageResult();
                    flushResults.forEach(result::withFlushResult);
                    if (result.getFlushedBytes() + result.getMergedBytes() + result.getFlushedAttributes() > 0) {
                        logStageEvent("Flush", result);
                    }

                    this.metrics.flushComplete(result.getFlushedBytes(), result.getMergedBytes(), result.getFlushedAttributes(), timer.getElapsed());
                    this.state.recordFlushComplete(result);
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "flush", traceId);
                }, this.executor);
    }

    /**
     * Cleans up all SegmentAggregators that are currently closed.
     */
    private void cleanup() {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "cleanup");
        val toRemove = this.processors.values().stream()
                                      .map(this::closeIfNecessary)
                                      .filter(ProcessorCollection::isClosed)
                                      .map(ProcessorCollection::getId)
                                      .collect(Collectors.toList());
        toRemove.forEach(this.processors::remove);
        LoggerHelpers.traceLeave(log, this.traceObjectId, "cleanup", traceId, toRemove.size());
    }

    /**
     * Closes the given ProcessorCollection if necessary.
     *
     * @param processorCollection The ProcessorCollection to test (and close if needed).
     * @return The same SegmentAggregator.
     */
    private ProcessorCollection closeIfNecessary(ProcessorCollection processorCollection) {
        if (processorCollection.shouldClose()) {
            processorCollection.close();
        }

        return processorCollection;
    }

    /**
     * Acknowledges operations that were flushed to storage
     */
    private void triggerAcknowledge() {
        checkRunning();
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "acknowledge");
        long highestCommittedSeqNo = this.ackCalculator.getHighestCommittedSequenceNumber(this.processors.values());
        long ackSequenceNumber = this.dataSource.getClosestValidTruncationPoint(highestCommittedSeqNo);

        if (ackSequenceNumber > this.state.getLastTruncatedSequenceNumber()) {
            this.ackProcessor.add(() -> {
                // If the StorageWriter completes an iteration faster than the data source can process the acknowledgment,
                // then the State's LastTruncatedSequenceNumber may not be updated in time and we can re-queue the same
                // truncation multiple times, which is undesirable. However, the ackProcessor serializes all invocations
                // to its add() method so at this point we are guaranteed to have completed the callback below that updates
                // that value.
                if (ackSequenceNumber <= this.state.getLastTruncatedSequenceNumber()) {
                    return CompletableFuture.completedFuture(null);
                }

                // Issue the truncation and update the state (when done).
                return this.dataSource
                        .acknowledge(ackSequenceNumber, this.config.getAckTimeout())
                        .thenRun(() -> {
                            this.state.setLastTruncatedSequenceNumber(ackSequenceNumber);
                            logStageEvent("Acknowledged", "SeqNo=" + ackSequenceNumber);
                            LoggerHelpers.traceLeave(log, this.traceObjectId, "acknowledge", traceId, ackSequenceNumber);
                        });
            }).exceptionally(this::iterationErrorHandler);
        } else {
            // Nothing to do.
            LoggerHelpers.traceLeave(log, this.traceObjectId, "acknowledge", traceId, Operation.NO_SEQUENCE_NUMBER);
        }
    }

    //endregion

    //region Helpers

    /**
     * Gets, or creates, a SegmentAggregator for the given StorageOperation.
     *
     * @param streamSegmentId The Id of the StreamSegment to get the aggregator for.
     */
    private CompletableFuture<ProcessorCollection> getProcessor(long streamSegmentId) {
        ProcessorCollection existingProcessor = this.processors.getOrDefault(streamSegmentId, null);
        if (existingProcessor != null) {
            if (closeIfNecessary(existingProcessor).isClosed()) {
                // Existing SegmentAggregator has become stale (most likely due to its SegmentMetadata being evicted),
                // so it has been closed and we need to create a new one.
                this.processors.remove(streamSegmentId);
            } else {
                return CompletableFuture.completedFuture(existingProcessor);
            }
        }

        // Get the SegmentAggregator's Metadata.
        UpdateableSegmentMetadata segmentMetadata = this.dataSource.getStreamSegmentMetadata(streamSegmentId);
        if (segmentMetadata == null) {
            return Futures.failedFuture(new DataCorruptionException(String.format(
                    "No StreamSegment with id '%d' is registered in the metadata.", streamSegmentId)));
        }

        // Then create the aggregator, and only register it after a successful initialization. Otherwise we risk
        // having a registered aggregator that is not initialized.
        SegmentAggregator segmentAggregator = new SegmentAggregator(segmentMetadata, this.dataSource, this.storage, this.config, this.timer, this.executor);
        AttributeAggregator attributeAggregator = segmentMetadata.getType().isTransientSegment() ? null :
                new AttributeAggregator(segmentMetadata, this.dataSource, this.config, this.timer, this.executor);
        ProcessorCollection pc = new ProcessorCollection(segmentAggregator, attributeAggregator, this.createProcessors.apply(segmentMetadata));
        try {
            CompletableFuture<Void> init = segmentAggregator.initialize(this.config.getFlushTimeout());
            Futures.exceptionListener(init, ex -> segmentAggregator.close());
            return init.thenApply(ignored -> {
                this.processors.put(streamSegmentId, pc);
                return pc;
            });
        } catch (Exception ex) {
            pc.close();
            throw ex;
        }
    }

    private boolean isCriticalError(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        return Exceptions.mustRethrow(ex)
                || ex instanceof ServiceHaltException     // Service halt or Data corruption - stop processing to prevent more damage.
                || ex instanceof StorageNotPrimaryException  // Fenced out - another instance took over.
                || ex instanceof DataLogWriterNotPrimaryException;  // Fenced out at the DurableLog level.
    }

    private boolean isShutdownException(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        return ex instanceof ObjectClosedException || ex instanceof CancellationException;
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
        for (ProcessorCollection a : this.processors.values()) {
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
        ex = Exceptions.unwrap(ex);
        if (critical) {
            log.error("{}: Iteration[{}].CriticalError.", this.traceObjectId, this.state.getIterationId(), ex);
        } else {
            log.error("{}: Iteration[{}].Error.", this.traceObjectId, this.state.getIterationId(), ex);
        }
        //System.out.println(String.format("%s: Iteration[%s].Error. %s", this.traceObjectId, this.state.getIterationId(), ex));
    }

    private void logErrorHandled(Throwable ex) {
        ex = Exceptions.unwrap(ex);
        log.warn("{}: Iteration[{}].HandledError {}", this.traceObjectId, this.state.getIterationId(), ex.toString());
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
    private static class FlushStageResult extends WriterFlushResult {
        int count;

        @Override
        public FlushStageResult withFlushResult(WriterFlushResult flushResult) {
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

        void operationProcessed(Operation op) {
            this.count++;
            if (op instanceof StorageOperation) {
                this.bytes += ((StorageOperation) op).getLength();
            }
        }

        @Override
        public String toString() {
            return String.format("Count=%d, Bytes=%d, LastReadSN=%d", this.count, this.bytes, this.state.getLastReadSequenceNumber());
        }
    }

    //endregion

    //region ProcessorCollection

    /**
     * Wraps a collection of WriterSegmentProcessors, including the main Segment Aggregator.
     */
    private class ProcessorCollection implements WriterSegmentProcessor {
        private final SegmentAggregator aggregator;
        private final List<WriterSegmentProcessor> processors;

        ProcessorCollection(SegmentAggregator aggregator, AttributeAggregator attributeAggregator, Collection<WriterSegmentProcessor> processors) {
            // We separate out the main SegmentAggregator since we depend on it for some operations, however when we
            // generate the list of processors we make sure to put it first; if there are any issues with the operations
            // to process we need to ensure that no other processor may see those operations before the Segment Aggregator.
            this.aggregator = aggregator;
            ImmutableList.Builder<WriterSegmentProcessor> builder = ImmutableList.<WriterSegmentProcessor>builder()
                    .add(aggregator)
                    .addAll(processors);
            if (attributeAggregator != null) {
                builder.add(attributeAggregator);
            }
            this.processors = builder.build();
        }

        //region SegmentAggregator direct wrapper

        /**
         * Gets a value indicating the amount of time since the main Segment Aggregator has been flushed.
         */
        Duration getElapsedSinceLastFlush() {
            return this.aggregator.getElapsedSinceLastFlush();
        }

        /**
         * Gets a value indicating the Segment Id for all processors in this collection.
         */
        long getId() {
            return this.aggregator.getMetadata().getId();
        }

        /**
         * Gets a value indicating whether the SegmentAggregator can be closed.
         */
        boolean shouldClose() {
            return this.aggregator.getMetadata().isDeletedInStorage() || !this.aggregator.getMetadata().isActive();
        }

        //endregion

        //region WriterSegmentProcessor Implementation

        @Override
        public void close() {
            this.processors.forEach(WriterSegmentProcessor::close);
        }

        @Override
        public boolean isClosed() {
            return this.processors.stream().allMatch(WriterSegmentProcessor::isClosed);
        }

        @Override
        public long getLowestUncommittedSequenceNumber() {
            return StorageWriter.this.ackCalculator.getLowestUncommittedSequenceNumber(this.processors);
        }

        @Override
        public boolean mustFlush() {
            return this.processors.stream().anyMatch(WriterSegmentProcessor::mustFlush);
        }

        @Override
        public void add(SegmentOperation operation) throws ServiceHaltException {
            for (WriterSegmentProcessor wsp : this.processors) {
                wsp.add(operation);
            }
        }

        @Override
        public CompletableFuture<WriterFlushResult> flush(boolean force, Duration timeout) {
            return Futures.allOfWithResults(this.processors.stream().map(wsp -> wsp.flush(force, timeout)).collect(Collectors.toList()))
                          .thenApply(results -> {
                              WriterFlushResult r = results.get(0);
                              for (int i = 1; i < results.size(); i++) {
                                  r.withFlushResult(results.get(i));
                              }
                              return r;
                          });
        }

        //endregion
    }

    //endregion
}
