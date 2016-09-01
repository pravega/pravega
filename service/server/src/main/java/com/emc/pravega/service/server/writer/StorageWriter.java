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
import com.emc.pravega.common.MathHelpers;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.RuntimeStreamingException;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.ServiceShutdownListener;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.Writer;
import com.emc.pravega.service.server.logs.operations.MergeBatchOperation;
import com.emc.pravega.service.server.logs.operations.MetadataCheckpointOperation;
import com.emc.pravega.service.server.logs.operations.MetadataOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractService;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
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
    private static final Duration MERGE_TIMEOUT = GENERAL_TIMEOUT;
    private static final Duration TRUNCATE_TIMEOUT = GENERAL_TIMEOUT;
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(10);
    private final String traceObjectId;
    private final WriterConfig config;
    private final UpdateableContainerMetadata containerMetadata;
    private final WriterDataSource dataSource;
    private final Storage storage;
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

    StorageWriter(WriterConfig config, UpdateableContainerMetadata containerMetadata, WriterDataSource dataSource, Storage storage, Executor executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(containerMetadata, "containerMetadata");
        Preconditions.checkNotNull(dataSource, "dataSource");
        Preconditions.checkNotNull(storage, "storage");
        Preconditions.checkNotNull(executor, "executor");

        this.traceObjectId = String.format("StorageWriter[%d]", containerMetadata.getContainerId());
        this.config = config;
        this.containerMetadata = containerMetadata;
        this.dataSource = dataSource;
        this.storage = storage;
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
        notifyStarted();
        this.executor.execute(this::runOneIteration);
        log.info("{} Started.", this.traceObjectId);
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
        logStageEvent(null, "Start");

        // A Writer iteration is made of the following stages:
        // 1. Read data.
        // 2. Load data into SegmentAggregators.
        // 3. Flush eligible SegmentAggregators.
        // 4. Merge eligible SegmentAggregators.
        // 5. Acknowledge (truncate).
        this.currentIteration = readData()
                .thenAcceptAsync(this::processReadResult, this.executor)
                .thenCompose(v -> this.flush())
                .thenCompose(v -> this.merge())
                .thenCompose(v -> this.acknowledge());

        // When the iteration is complete, process its result and start a new iteration, if needed.
        this.currentIteration.whenComplete(this::endOfIteration);
    }

    /**
     * Called when an iteration is complete, whether successfully or not.
     *
     * @param ignored Not used.
     * @param ex      (Optional) An exception that was thrown during the execution of the iteration.
     */
    private void endOfIteration(Void ignored, Throwable ex) {
        this.currentIteration = null;
        if (ex != null) {
            if (ExceptionHelpers.getRealException(ex) instanceof CancellationException && !isRunning()) {
                // Writer is not running and we caught a CancellationException.
                // This is a normal behavior and it is triggered by stopAsync(); just exit without logging or triggering anything else.
                return;
            }

            boolean critical = isCriticalError(ex);
            ExceptionHelpers.getRealException(ex).printStackTrace(System.err);
            log.error("{}: Iteration[{}].Error[Critical={}]. {}", this.traceObjectId, this.iterationId, critical, ex);
            System.out.println(String.format("%s: Iteration[%s].Error[Critical=%s]. %s", this.traceObjectId, this.iterationId, critical, ex));
            if (critical) {
                this.stopException.set(ex);
                stopAsync();
                return;
            }
        }

        logStageEvent(null, "Finish");
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
        try {
            Duration readTimeout = getReadTimeout();
            return this.dataSource.read(this.state.getLastReadSequenceNumber(), this.config.getMaxItemsToReadAtOnce(), readTimeout);
        } catch (Throwable ex) {
            // This is for synchronous exceptions; endOfIteration() will take care of this.
            return FutureHelpers.failedFuture(ex);
        }
    }

    /**
     * Processes all the operations in the given ReadResult.
     *
     * @param readResult The read result to process.
     */
    private void processReadResult(Iterator<Operation> readResult) {
        InputReadStageResult result = new InputReadStageResult(this.state);
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

        logStageEvent(result, "InputRead");
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

    private long processStorageOperation(StorageOperation op) throws DataCorruptionException {
        // Add the operation to the appropriate Aggregator.
        SegmentAggregator aggregator = getSegmentAggregator(op.getStreamSegmentId());
        aggregator.add(op);
        if (op instanceof MergeBatchOperation) {
            // If a Batch, it needs to be added both to the Parent StreamSegment and to the batch StreamSegment.
            aggregator = getSegmentAggregator(((MergeBatchOperation) op).getBatchStreamSegmentId());
            aggregator.add(op);
        }

        return op.getLength();
    }

    //endregion

    /**
     * Flushes eligible operations to Storage, if necessary. Does not perform any mergers.
     */
    private CompletableFuture<Void> flush() {
        checkRunning();

        // Flush everything we can flush.
        ArrayList<CompletableFuture<FlushResult>> flushFutures = new ArrayList<>();
        for (SegmentAggregator a : this.aggregators.values()) {
            if (a.mustFlush()) {
                // We have a Segment that is ripe for flushing.
                flushFutures.add(a.flush(FLUSH_TIMEOUT, this.executor));
            }
        }

        return completeStage(flushFutures, "Flush");
    }

    /**
     * Executes any mergers for StreamSegments that are eligible.
     */
    private CompletableFuture<Void> merge() {
        checkRunning();

        ArrayList<CompletableFuture<FlushResult>> mergeResults = new ArrayList<>();
        ArrayList<Long> mergedSegments = new ArrayList<>();
        for (SegmentAggregator aggregator : this.aggregators.values()) {
            if (aggregator.getMetadata().getParentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID || !aggregator.canMerge()) {
                // Not a batch or a a batch that is not yet ready to merge.
                continue;
            }

            SegmentAggregator parentAggregator = this.aggregators.get(aggregator.getMetadata().getParentId());
            assert parentAggregator != null : "No parent SegmentAggregator found for a SegmentAggregator ready to merge.";
            if (parentAggregator.canMergeWith(aggregator)) {
                // This stand-alone SegmentAggregator is ready to merge with our batch SegmentAggregator.
                // Execute the merge, then update the metadata with the fact.
                CompletableFuture<FlushResult> mergeFuture = parentAggregator
                        .mergeWith(aggregator, this.storage, MERGE_TIMEOUT)
                        .thenApplyAsync(result -> {
                            updateMetadataPostMerge(aggregator);

                            // Close & remove the SegmentAggregator from our map.
                            aggregator.close();
                            synchronized (mergedSegments) {
                                mergedSegments.add(aggregator.getMetadata().getId());
                            }

                            return result;
                        }, this.executor);
                mergeResults.add(mergeFuture);
            }
        }

        return completeStage(mergeResults, "Merge")
                .thenRun(() -> mergedSegments.forEach(this.aggregators::remove));
    }

    /**
     * Updates the Container Metadata, as well as the StorageWriter's state with the result of the merger of the given
     * batch SegmentAggregator. This operation has the following effects:
     * <ul>
     * <li> The SegmentAggregator is removed from the StorageWriter's memory.
     * <li> The Batch StreamSegment is removed from the ContainerMetadata.
     * <li> The merger is marked "complete" in the ReadIndex.
     * </ul>
     *
     * @param batchSegmentAggregator The SegmentAggregator that was just merged.
     */
    private void updateMetadataPostMerge(SegmentAggregator batchSegmentAggregator) {
        SegmentMetadata batchSegmentMetadata = batchSegmentAggregator.getMetadata();
        assert batchSegmentMetadata.getParentId() != ContainerMetadata.NO_STREAM_SEGMENT_ID : "given batchSegmentAggregator does not refer to a batch Segment";
        assert batchSegmentMetadata.isMerged() : "given batchSegmentAggregator does not refer to a merged batch Segment";
        assert batchSegmentMetadata.isDeleted() : "given batchSegmentAggregator does not refer to a deleted Segment";

        // Delete the batch StreamSegment from the metadata.
        this.containerMetadata.deleteStreamSegment(batchSegmentMetadata.getName());

        // Complete the merger (in the ReadIndex and whatever other listeners we might have).
        this.dataSource.completeMerge(batchSegmentMetadata.getParentId(), batchSegmentMetadata.getId());
    }

    /**
     * Acknowledges operations that were flushed to storage
     */
    private CompletableFuture<Void> acknowledge() {
        checkRunning();

        // The Sequence Number we acknowledge has the property that all operations up to, and including it, have been
        // committed to Storage.
        // This can only be calculated by looking at all the active SegmentAggregators and picking the Lowest Uncommitted
        // Sequence Number (LUSN) among all of those Aggregators that have any outstanding data. The LUSN for each aggregator
        // has the property that, within the context of that Aggregator alone, all Operations that have a Sequence Number (SN)
        // smaller than LUSN have been committed to Storage. As such, picking the smallest of all LUSN values across
        // all the active SegmentAggregators will give us the highest SN that can be safely truncated out of the OperationLog.
        // Note that LUSN still points to an uncommitted Operation, so we need to subtract 1 from it to obtain the highest SN
        // that can be truncated up to (and including).
        // If we have no active Aggregators, then we have committed all operations that were passed to us, so we can
        // safely truncate up to LastReadSequenceNumber.

        long lowestUncommittedSeqNo = Long.MAX_VALUE;
        for (SegmentAggregator a : this.aggregators.values()) {
            long firstSeqNo = a.getLowestUncommittedSequenceNumber();
            if (firstSeqNo >= 0) {
                lowestUncommittedSeqNo = Math.min(lowestUncommittedSeqNo, firstSeqNo);
            }
        }

        // Subtract 1 from the computed LUSN and then make sure it doesn't exceed the LastReadSequenceNumber
        // (it would only exceed it if there are no aggregators or of they are all empty - which means we processed everything).
        lowestUncommittedSeqNo = Math.min(lowestUncommittedSeqNo - 1, this.state.getLastReadSequenceNumber());

        this.state.setLowestUncommittedSequenceNumber(lowestUncommittedSeqNo);
        long truncationSequenceNumber = this.containerMetadata.getClosestValidTruncationPoint(lowestUncommittedSeqNo);
        if (truncationSequenceNumber > this.state.getLastTruncatedSequenceNumber()) {
            // Issue the truncation and update the state (when done).
            return this.dataSource
                    .acknowledge(truncationSequenceNumber, TRUNCATE_TIMEOUT)
                    .thenRun(() -> this.state.setLastTruncatedSequenceNumber(truncationSequenceNumber));
        } else {
            // Nothing to do.
            return CompletableFuture.completedFuture(null);
        }
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
        result = this.aggregators.getOrDefault(streamSegmentId, null);
        if (result == null) {
            // We do not yet have this aggregator. First, get its metadata.
            UpdateableSegmentMetadata segmentMetadata = this.containerMetadata.getStreamSegmentMetadata(streamSegmentId);
            if (segmentMetadata == null) {
                throw new DataCorruptionException(String.format("No StreamSegment with id '%d' is registered in the metadata.", streamSegmentId));
            }

            // Then create the aggregator.
            result = new SegmentAggregator(segmentMetadata, this.storage, this.dataSource, this.config, this.stopwatch);
            this.aggregators.put(streamSegmentId, result);
            result.initialize(GENERAL_TIMEOUT).join();
        }

        return result;
    }

    private boolean isCriticalError(Throwable ex) {
        return ExceptionHelpers.mustRethrow(ex)
                || ExceptionHelpers.getRealException(ex) instanceof DataCorruptionException;
    }

    /**
     * Calculates the amount of time until the first SegmentAggregator will expire (needs to flush). If no Aggregator
     * is registered, a default (large) value is returned.
     *
     * @return The calculated value.
     */
    private Duration getReadTimeout() {
        // Find the minimum expiration time among all SegmentAggregators.
        long timeMillis = this.config.getMaxReadTimeout().toMillis();
        long minTimeMillis = this.config.getMinReadTimeout().toMillis();
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
     * Waits for all the stage components to finish, aggregates their results, and logs the stage completion event in the log.
     *
     * @param stageComponents The stage components to wait for.
     * @param stageName       The name of the stage (used for logging)
     * @return A CompletableFuture that will complete (or fail) when all the stage components complete, or any fails.
     */
    private CompletableFuture<Void> completeStage(Collection<CompletableFuture<FlushResult>> stageComponents, String stageName) {
        return FutureHelpers
                .allOfWithResults(stageComponents)
                .thenAccept(flushResults -> {
                    StageResult result = new StageResult();
                    flushResults.forEach(r -> result.addBytes(r.getLength()));
                    if (result.bytes + result.count > 0) {
                        logStageEvent(result, stageName);
                    }
                });
    }

    private void logStageEvent(StageResult result, String stageName) {
        if (result == null) {
            log.debug("{}: Iteration[{}].{}.", this.traceObjectId, this.iterationId, stageName);
            System.out.println(String.format("%s: Iteration[%s].%s.", this.traceObjectId, this.iterationId, stageName));
        } else {
            log.debug("{}: Iteration[{}].{} ({}).", this.traceObjectId, this.iterationId, stageName, result);
            System.out.println(String.format("%s: Iteration[%s].%s (%s).", this.traceObjectId, this.iterationId, stageName, result));
        }
    }

    private void checkRunning() {
        if (!isRunning()) {
            throw new CancellationException("StorageWriter has been stopped.");
        }
    }

    //region StageResult

    private static class StageResult {
        int count;
        long bytes;

        void addBytes(long byteCount) {
            this.bytes += byteCount;
            this.count++;
        }

        @Override
        public String toString() {
            return String.format("Count=%d, Bytes=%d", this.count, this.bytes);
        }
    }

    private static class InputReadStageResult extends StageResult {
        private final WriterState state;

        InputReadStageResult(WriterState state) {
            this.state = state;
        }

        @Override
        public String toString() {
            return String.format("%s, LastReadSN=%d", super.toString(), this.state.getLastReadSequenceNumber());
        }
    }

    //endregion
}
