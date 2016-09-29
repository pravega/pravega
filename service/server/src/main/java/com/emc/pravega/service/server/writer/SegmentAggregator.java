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
import com.emc.pravega.common.TimeoutTimer;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.config.WriterConfig;
import com.emc.pravega.service.contracts.RuntimeStreamingException;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentNotExistsException;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.server.CacheKey;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.ExceptionHelpers;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.logs.operations.CachedStreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.MergeBatchOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentSealOperation;
import com.emc.pravega.service.storage.BadOffsetException;
import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Aggregates contents for a specific StreamSegment.
 */
@Slf4j
class SegmentAggregator implements OperationProcessor, AutoCloseable {
    //region Members

    private final UpdateableSegmentMetadata metadata;
    private final WriterConfig config;
    private final Queue<StorageOperation> operations;
    private final AutoStopwatch stopwatch;
    private final String traceObjectId;
    private final Storage storage;
    private final WriterDataSource dataSource;
    private Duration lastFlush;
    private final AtomicLong outstandingAppendLength;
    private final AtomicInteger mergeBatchCount;
    private final AtomicBoolean hasSealPending;
    private long lastAddedOffset;
    private boolean closed;
    private boolean isInitialized;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SegmentAggregator class.
     *
     * @param segmentMetadata The Metadata for the StreamSegment to construct this Aggregator for.
     * @param dataSource      The WriterDataSource to use.
     * @param storage         The Storage to use (for flushing).
     * @param config          The Configuration to use.
     * @param stopwatch       A Stopwatch to use to determine elapsed time.
     */
    SegmentAggregator(UpdateableSegmentMetadata segmentMetadata, WriterDataSource dataSource, Storage storage, WriterConfig config, AutoStopwatch stopwatch) {
        Preconditions.checkNotNull(segmentMetadata, "segmentMetadata");
        Preconditions.checkNotNull(dataSource, "dataSource");
        Preconditions.checkNotNull(storage, "storage");
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(stopwatch, "stopwatch");

        this.metadata = segmentMetadata;
        Preconditions.checkArgument(this.metadata.getContainerId() == dataSource.getId(), "SegmentMetadata.ContainerId is different from WriterDataSource.Id");

        this.config = config;
        this.storage = storage;
        this.dataSource = dataSource;
        this.stopwatch = stopwatch;
        this.lastFlush = stopwatch.elapsed();
        this.outstandingAppendLength = new AtomicLong();
        this.lastAddedOffset = -1; // Will be set properly in initialize().
        this.mergeBatchCount = new AtomicInteger();
        this.hasSealPending = new AtomicBoolean();
        this.operations = new ConcurrentLinkedQueue<>();
        this.traceObjectId = String.format("StorageWriter[%d-%d]", this.metadata.getContainerId(), this.metadata.getId());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            log.info("{}: Closed.");
            this.closed = true;
        }
    }

    //endregion

    //region OperationProcessor Implementation

    @Override
    public long getLowestUncommittedSequenceNumber() {
        StorageOperation first = this.operations.peek();
        return first == null ? Operation.NO_SEQUENCE_NUMBER : first.getSequenceNumber();
    }

    @Override
    public boolean isClosed() {
        return this.closed;
    }

    //endregion

    //region Properties

    /**
     * Gets a reference to the SegmentMetadata related to this Aggregator.
     *
     * @return The metadata.
     */
    SegmentMetadata getMetadata() {
        return this.metadata;
    }

    /**
     * Gets a value representing the amount of time since the last successful call to flush(). If no such call has been
     * made yet, this returns the amount of time since the creation of this SegmentAggregator object.
     *
     * @return The result.
     */
    Duration getElapsedSinceLastFlush() {
        return this.stopwatch.elapsed().minus(this.lastFlush);
    }

    /**
     * Gets a value indicating whether a call to flush() is required given the current state of this SegmentAggregator.
     * <p>
     * Any of the following conditions can trigger a flush:
     * <ul>
     * <li> There is more data in the SegmentAggregator than the configuration allows (getOutstandingLength >= FlushThresholdBytes)
     * <li> Too much time has passed since the last call to flush() (getElapsedSinceLastFlush >= FlushThresholdTime)
     * <li> The SegmentAggregator contains a StreamSegmentSealOperation or MergeBatchOperation (hasSealPending == true)
     * </ul>
     *
     * @return The result.
     */
    boolean mustFlush() {
        return exceedsThresholds()
                || this.hasSealPending.get()
                || this.mergeBatchCount.get() > 0;
    }

    /**
     * Gets a value indicating whether the Flush thresholds are exceeded for this SegmentAggregator.
     *
     * @return The result.
     */
    private boolean exceedsThresholds() {
        long length = this.outstandingAppendLength.get();
        return length >= this.config.getFlushThresholdBytes()
                || (length > 0 && getElapsedSinceLastFlush().compareTo(this.config.getFlushThresholdTime()) >= 0);
    }

    @Override
    public String toString() {
        return String.format(
                "[%d: %s] Size = %d|%s, LastOffset = %d, LUSN = %d LastFlush = %ds",
                this.metadata.getId(),
                this.metadata.getName(),
                this.operations.size(),
                this.outstandingAppendLength,
                this.lastAddedOffset,
                getLowestUncommittedSequenceNumber(),
                this.getElapsedSinceLastFlush().toMillis() / 1000);
    }

    //endregion

    //region Operations

    /**
     * Initializes the SegmentAggregator by pulling information from the given Storage.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation finished successfully. If any
     * errors occurred during the operation, the Future will be completed with the appropriate exception.
     */
    CompletableFuture<Void> initialize(Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!this.isInitialized, "SegmentAggregator has already been initialized.");

        return this.storage
                .getStreamSegmentInfo(this.metadata.getName(), timeout)
                .thenAccept(segmentInfo -> {
                    // Check & Update StorageLength in metadata.
                    if (this.metadata.getStorageLength() != segmentInfo.getLength()) {
                        if (this.metadata.getStorageLength() >= 0) {
                            // Only log warning if the StorageLength has actually been initialized, but is different.
                            log.warn("{}: SegmentMetadata has a StorageLength ({}) that is different than the actual one ({}) - updating metadata.", this.traceObjectId, this.metadata.getStorageLength(), segmentInfo.getLength());
                        }

                        // It is very important to keep this value up-to-date and correct.
                        this.metadata.setStorageLength(segmentInfo.getLength());
                    }

                    // Check if the Storage segment is sealed, but it's not in metadata (this is 100% indicative of some data corruption happening).
                    if (segmentInfo.isSealed()) {
                        if (!this.metadata.isSealed()) {
                            throw new RuntimeStreamingException(new DataCorruptionException(String.format("Segment '%s' is sealed in Storage but not in the metadata.", this.metadata.getName())));
                        }

                        if (!this.metadata.isSealedInStorage()) {
                            this.metadata.markSealedInStorage();
                            log.warn("{}: Segment is sealed in Storage but metadata does not reflect that - updating metadata.", this.traceObjectId, segmentInfo.getLength());
                        }
                    }

                    log.info("{}: Initialized. StorageLength = {}, Sealed = {}.", this.traceObjectId, segmentInfo.getLength(), segmentInfo.isSealed());
                    this.isInitialized = true;
                })
                .exceptionally(ex -> {
                    ex = ExceptionHelpers.getRealException(ex);
                    if (ex instanceof StreamSegmentNotExistsException) {
                        // Segment does not exist anymore. This is a real possibility during recovery, in the following cases:
                        // * We already processed a Segment Deletion but did not have a chance to checkpoint metadata
                        // * We processed a BatchMergeOperation but did not have a chance to ack/truncate the DataSource
                        this.metadata.markDeleted(); // Update metadata, just in case it is not already updated.
                        log.warn("{}: Segment does not exist in Storage. Ignoring all further operations on it.", this.traceObjectId, ex);
                    } else {
                        // Other kind of error - re-throw.
                        throw new CompletionException(ex);
                    }

                    this.isInitialized = true;
                    return null;
                });
    }

    /**
     * Adds the given StorageOperation to the Aggregator.
     *
     * @param operation the Operation to add.
     * @throws DataCorruptionException  If the validation of the given Operation indicates a possible data corruption in
     *                                  the code (offset gaps, out-of-order operations, etc.)
     * @throws IllegalArgumentException If the validation of the given Operation indicates a possible non-corrupting bug
     *                                  in the code.
     */
    void add(StorageOperation operation) throws DataCorruptionException {
        ensureInitializedAndNotClosed();

        // Verify operation Segment Id.
        checkSegmentId(operation);

        if (this.metadata.isDeleted()) {
            // Deleted Segment - nothing to do.
            return;
        }

        // Verify operation validity (this also takes care of extra operations after Seal or Merge; no need for further checks).
        checkValidOperation(operation);

        // Add operation to list, but only if hasn't yet been persisted in Storage.
        // We can figure this out if we compare the last offset of the op with Metadata.StorageLength or, for seal operations,
        // if it hasn't already been sealed in Storage.
        long lastOffset = operation.getLastStreamSegmentOffset();
        boolean processOp = lastOffset > this.metadata.getStorageLength()
                || (!this.metadata.isSealedInStorage() && (operation instanceof StreamSegmentSealOperation));
        if (processOp) {
            this.operations.add(operation);
            if (operation instanceof MergeBatchOperation) {
                this.mergeBatchCount.incrementAndGet();
            } else if (operation instanceof StreamSegmentSealOperation) {
                this.hasSealPending.set(true);
            } else if (operation instanceof StreamSegmentAppendOperation || operation instanceof CachedStreamSegmentAppendOperation) {
                // Update current outstanding length - but we only keep track of this for appends (MergeBatches do not count for flush thresholds).
                this.outstandingAppendLength.addAndGet(operation.getLength());
            }
        }

        // Always record the last added offset, to ensure that operations are processed in the right order.
        this.lastAddedOffset = lastOffset;
    }

    //endregion

    //region Flushing and Merging

    /**
     * Flushes the contents of the Aggregator to the given Storage.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a summary of the flush operation. If any errors
     * occurred during the flush, the Future will be completed with the appropriate exception.
     */
    CompletableFuture<FlushResult> flush(Duration timeout) {
        ensureInitializedAndNotClosed();

        try {
            TimeoutTimer timer = new TimeoutTimer(timeout);
            boolean hasMerge = this.mergeBatchCount.get() > 0;
            boolean hasSeal = this.hasSealPending.get();
            if (hasSeal || hasMerge) {
                // If we have a Seal or Merge Pending, flush everything until we reach that operation.
                CompletableFuture<FlushResult> result = flushFully(timer);
                if (hasMerge) {
                    result = result.thenCompose(flushResult -> mergeIfNecessary(flushResult, timer));
                }

                if (hasSeal) {
                    result = result.thenCompose(flushResult -> sealIfNecessary(flushResult, timer));
                }

                return result;
            } else {
                // Otherwise, just flush the excess as long as we have something to flush.
                return flushExcess(timer);
            }
        } catch (Exception ex) {
            return FutureHelpers.failedFuture(ex);
        }
    }

    /**
     * Flushes all Append Operations that can be flushed at the given moment (until the entire Aggregator is emptied out
     * or until a StreamSegmentSealOperation or MergeBatchOperation is encountered).
     *
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation.
     * @throws DataCorruptionException If a CachedStreamSegmentAppendOperation does not have any data in the cache.
     */
    private CompletableFuture<FlushResult> flushFully(TimeoutTimer timer) throws DataCorruptionException {
        return flushConditionally(timer, () -> {
            StorageOperation first = this.operations.peek();
            return first != null && isAppendOperation(first);
        });
    }

    /**
     * Flushes as many Append Operations as needed as long as the data inside this SegmentAggregator exceeds size/time thresholds.
     * This will stop when either the thresholds are not exceeded anymore or when a non-Append Operation is encountered.
     *
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation.
     * @throws DataCorruptionException If a CachedStreamSegmentAppendOperation does not have any data in the cache.
     */
    private CompletableFuture<FlushResult> flushExcess(TimeoutTimer timer) throws DataCorruptionException {
        return flushConditionally(timer, this::exceedsThresholds);
    }

    /**
     * Flushes all Append Operations that can be flushed at the given moment (as long ast he given condition holds true).
     *
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation.
     * @throws DataCorruptionException If a CachedStreamSegmentAppendOperation does not have any data in the cache.
     */
    private CompletableFuture<FlushResult> flushConditionally(TimeoutTimer timer, Supplier<Boolean> condition) throws DataCorruptionException {
        FlushResult result = new FlushResult();

        // Flush all outstanding data as long as the threshold is exceeded.
        while (condition.get()) {
            // TODO: figure out how to get rid of this join. Is there something like an AsyncLoop with Futures?
            FlushResult partialFlushResult = flushOnce(timer.getRemaining()).join();
            result.withFlushResult(partialFlushResult);
        }

        return CompletableFuture.completedFuture(result);
    }

    /**
     * Flushes all Append Operations that can be flushed up to the maximum allowed flush size.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation.
     * @throws DataCorruptionException If a CachedStreamSegmentAppendOperation does not have any data in the cache.
     */
    private CompletableFuture<FlushResult> flushOnce(Duration timeout) throws DataCorruptionException {
        // Gather an InputStream made up of all the operations we can flush.
        FlushArgs flushArgs = getFlushArgs();

        if (flushArgs.getTotalLength() == 0) {
            // Nothing to flush.
            return CompletableFuture.completedFuture(new FlushResult());
        }

        // Flush them.
        InputStream inputStream = flushArgs.getInputStream();
        return this.storage
                .write(this.metadata.getName(), this.metadata.getStorageLength(), inputStream, flushArgs.getTotalLength(), timeout)
                .thenApply(v -> updateStatePostFlush(flushArgs))
                .exceptionally(ex -> {
                    if (ExceptionHelpers.getRealException(ex) instanceof BadOffsetException) {
                        // This is a bad one. We attempted to write at an offset that already contained other data.
                        // TODO: when we implement BadOffset Reconciliation, consider starting from here.
                        ex = new DataCorruptionException(String.format(
                                "Attempted to write at offset %d that is not the end of the segment in storage (Segment=%s).",
                                this.metadata.getStorageLength(), this.metadata.getName()), ex);
                    }

                    // Rethrow all exceptions.
                    throw new CompletionException(ex);
                });
    }

    /**
     * Aggregates all outstanding Append Operations into a single object that can be used for flushing. This continues
     * to aggregate operations until a non-Append operation is encountered or until we have accumulated enough data (exceeding config.getMaxFlushSizeBytes).
     *
     * @return The aggregated object that can be used for flushing.
     * @throws DataCorruptionException If a CachedStreamSegmentAppendOperation does not have any data in the cache.
     */
    private FlushArgs getFlushArgs() throws DataCorruptionException {
        FlushArgs result = new FlushArgs();
        int remainingCapacity = this.config.getMaxFlushSizeBytes();
        for (StorageOperation op : this.operations) {
            byte[] data;
            if (op instanceof StreamSegmentAppendOperation) {
                data = ((StreamSegmentAppendOperation) op).getData();
            } else if (op instanceof CachedStreamSegmentAppendOperation) {
                CacheKey key = ((CachedStreamSegmentAppendOperation) op).getCacheKey();
                data = this.dataSource.getAppendData(key);
                if (data == null) {
                    throw new DataCorruptionException(String.format("Unable to retrieve CacheContents for operation '%s', with key '%s'.", op, key));
                }
            } else {
                // We found one operation that is not an append; this is as much as we can flush.
                break;
            }

            // Calculate the exact offset within the buffer that we need to start adding at. This is necessary because
            // we can commit partial operations (for optimization purposes).
            long bufferOffset = Math.max(0, this.metadata.getStorageLength() - op.getStreamSegmentOffset());
            if (bufferOffset > op.getLength()) {
                throw new DataCorruptionException(String.format("Cannot flush operation '%s'. Its end offset is before the Segment's StorageLength (%d).", op, this.metadata.getStorageLength()));
            }

            // Calculate the maximum amount of data from this buffer that we can commit, without exceeding the Max Flush Size (remainingCapacity).
            int bufferLength = Math.min(remainingCapacity, data.length - (int) bufferOffset);

            // Add whatever we can to the result.
            result.add(data, (int) bufferOffset, bufferLength);
            remainingCapacity -= bufferLength;

            if (remainingCapacity <= 0) {
                // We have exceeded the maximum flush size. Stop here and return the result.
                break;
            }
        }

        return result;
    }

    /**
     * Executes a merger of a Batch StreamSegment into this one.
     * Conditions for merger:
     * <ul>
     * <li> This StreamSegment is stand-alone (not a batch).
     * <li> The next outstanding operation is a MergeBatchOperation for a BatchStreamSegment of this StreamSegment.
     * <li> The StreamSegment to merge is not deleted, it is sealed and is fully flushed to Storage.
     * </ul>
     * Effects of the merger:
     * <ul> The entire contents of the given batch StreamSegment will be concatenated to this StreamSegment as one unit.
     * <li> The metadata for this StreamSegment will be updated to reflect the new length of this StreamSegment.
     * <li> The given batch Segment will cease to exist.
     * </ul>
     * <p>
     * Note that various other data integrity checks are done pre and post merger as part of this operation which are meant
     * to ensure the StreamSegment is not in a corrupted state.
     *
     * @param flushResult The flush result from the previous chained operation.
     * @param timer       Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the number of bytes that were merged into this
     * StreamSegment. If failed, the Future will contain the exception that caused it.
     */
    private CompletableFuture<FlushResult> mergeIfNecessary(FlushResult flushResult, TimeoutTimer timer) {
        ensureInitializedAndNotClosed();
        assert this.metadata.getParentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID : "Cannot merge into a Batch StreamSegment.";

        StorageOperation first = this.operations.peek();
        if (first == null || !(first instanceof MergeBatchOperation)) {
            // Either no operation or first operation is not a MergeBatch. Nothing to do.
            return CompletableFuture.completedFuture(flushResult);
        }

        // TODO: This only processes one merge at a time. If we had several, that would mean each is done in a different iteration. Should we improve this?
        MergeBatchOperation mergeBatchOperation = (MergeBatchOperation) first;
        UpdateableSegmentMetadata batchMetadata = this.dataSource.getStreamSegmentMetadata(mergeBatchOperation.getBatchStreamSegmentId());
        return mergeWith(batchMetadata, timer)
                .thenApply(flushResult::withFlushResult);
    }

    /**
     * Merges the batch StreamSegment with given metadata into this one at the current offset.
     *
     * @param batchMetadata The metadata of the batchStreamSegment to merge.
     * @param timer         Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the number of bytes that were merged into this
     * StreamSegment. If failed, the Future will contain the exception that caused it.
     */
    private CompletableFuture<FlushResult> mergeWith(UpdateableSegmentMetadata batchMetadata, TimeoutTimer timer) {
        if (batchMetadata.isDeleted()) {
            return FutureHelpers.failedFuture(new DataCorruptionException(String.format("Attempted to merge with deleted batch segment '%s'.", batchMetadata.getName())));
        }

        FlushResult result = new FlushResult();
        if (!batchMetadata.isSealedInStorage() || batchMetadata.getDurableLogLength() > batchMetadata.getStorageLength()) {
            // Nothing to do. Given Batch is not eligible for merger yet.
            return CompletableFuture.completedFuture(result);
        }

        AtomicLong mergedLength = new AtomicLong();
        return this.storage
                .getStreamSegmentInfo(batchMetadata.getName(), timer.getRemaining())
                .thenAccept(batchSegmentProperties -> {
                    // One last verification before the actual merger:
                    // Check that the Storage agrees with our metadata (if not, we have a problem ...)
                    if (batchSegmentProperties.getLength() != batchMetadata.getStorageLength()) {
                        throw new CompletionException(new DataCorruptionException(String.format(
                                "Batch Segment '%s' cannot be merged into parent '%s' because its metadata disagrees with the Storage. Metadata.StorageLength=%d, Storage.StorageLength=%d",
                                batchMetadata.getName(),
                                this.metadata.getName(),
                                batchMetadata.getStorageLength(),
                                batchSegmentProperties.getLength())));
                    }

                    mergedLength.set(batchSegmentProperties.getLength());
                })
                .thenCompose(v1 -> storage.concat(this.metadata.getName(), batchMetadata.getName(), timer.getRemaining()))
                .thenCompose(v2 -> storage.getStreamSegmentInfo(this.metadata.getName(), timer.getRemaining()))
                .thenApply(segmentProperties -> {
                    // We have processed a MergeBatchOperation, pop the first operation off and decrement the counter.
                    StorageOperation processedOperation = this.operations.poll();
                    assert processedOperation != null && processedOperation instanceof MergeBatchOperation : "First outstanding operation was not a MergeBatchOperation";
                    assert ((MergeBatchOperation) processedOperation).getBatchStreamSegmentId() == batchMetadata.getId() : "First outstanding operation was a MergeBatchOperation for the wrong batch id.";
                    int newCount = this.mergeBatchCount.decrementAndGet();
                    assert newCount >= 0 : "Negative value for mergeBatchCount";

                    // Post-merger validation. Verify we are still in agreement with the storage.
                    long expectedNewLength = this.metadata.getStorageLength() + mergedLength.get();
                    if (segmentProperties.getLength() != expectedNewLength) {
                        throw new CompletionException(new DataCorruptionException(String.format(
                                "Batch Segment '%s' was merged into parent '%s' but the parent segment has an unexpected StorageLength after the merger. Previous=%d, MergeLength=%d, Expected=%d, Actual=%d",
                                batchMetadata.getName(),
                                this.metadata.getName(),
                                segmentProperties.getLength(),
                                mergedLength.get(),
                                expectedNewLength,
                                segmentProperties.getLength())));
                    }

                    updateMetadata(segmentProperties);
                    updateMetadataForBatchPostMerger(batchMetadata);

                    this.lastFlush = this.stopwatch.elapsed();
                    return result.withMergedBytes(mergedLength.get());
                });
    }

    /**
     * Seals the StreamSegment in Storage, if necessary.
     *
     * @param flushResult The FlushResult from a previous Flush operation. This will just be passed-through.
     * @param timer       Timer for the operation.
     * @return The FlushResult passed in as an argument.
     */
    private CompletableFuture<FlushResult> sealIfNecessary(FlushResult flushResult, TimeoutTimer timer) {
        if (!this.hasSealPending.get() || !(this.operations.peek() instanceof StreamSegmentSealOperation)) {
            // Either no Seal is pending or the next operation is not a seal - we cannot execute a seal.
            return CompletableFuture.completedFuture(flushResult);
        }

        return this.storage
                .seal(this.metadata.getName(), timer.getRemaining())
                .handle((v, ex) -> {
                    if (ex != null && !(ExceptionHelpers.getRealException(ex) instanceof StreamSegmentSealedException)) {
                        // The operation failed, and it was not because the Segment was already Sealed. Throw it again.
                        // We consider the Seal to succeed if the Segment in Storage is already sealed - it's an idempotent operation.
                        throw new CompletionException(ex);
                    }

                    // Update metadata.
                    this.metadata.markSealedInStorage();
                    this.operations.poll();

                    // Validate we have no more unexpected items and then close (as we shouldn't be getting anything else).
                    assert this.operations.size() == 0 : "Processed StreamSegmentSeal operation but more operations are outstanding.";
                    this.hasSealPending.set(false);
                    close();
                    return flushResult;
                });
    }

    //endregion

    //region Helpers

    /**
     * Ensures the following conditions are met:
     * * Regular Operations: SegmentId matches this SegmentAggregator's SegmentId
     * * Batches: TargetSegmentId/SegmentId matches this SegmentAggregator's SegmentId.
     *
     * @param operation The operation to check.
     * @throws IllegalArgumentException If any of the validations failed.
     */
    private void checkSegmentId(StorageOperation operation) {
        // All exceptions thrown from here are RuntimeExceptions (as opposed from DataCorruptionExceptions); they are indicative
        // of bad code (objects got routed to wrong SegmentAggregators) and not data corruption.
        if (operation instanceof MergeBatchOperation) {
            Preconditions.checkArgument(
                    this.metadata.getParentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID,
                    "MergeBatchOperations can only be added to the parent StreamSegment; received '%s'.", operation);

            // Since we are a stand-alone StreamSegment; verify that the Operation has us as a parent (target).
            Preconditions.checkArgument(
                    operation.getStreamSegmentId() == this.metadata.getId(),
                    "Operation '%s' refers to a different StreamSegment as a target (parent) than this one (%s).", operation, this.metadata.getId());
        } else {
            // Regular operation.
            Preconditions.checkArgument(
                    operation.getStreamSegmentId() == this.metadata.getId(),
                    "Operation '%s' refers to a different StreamSegment than this one (%s).", operation, this.metadata.getId());
        }
    }

    /**
     * Ensures the following conditions are met:
     * * Operation Offset matches the last Offset from the previous operation (that is, operations are contiguous).
     *
     * @param operation The operation to check.
     * @throws DataCorruptionException  If any of the validations failed.
     * @throws IllegalArgumentException If the operation has an undefined Offset or Length (these are not considered data-
     *                                  corrupting issues).
     */
    private void checkValidOperation(StorageOperation operation) throws DataCorruptionException {
        boolean hasSeal = this.hasSealPending.get();
        if (hasSeal) {
            // After a StreamSegmentSeal, we do not allow any other operation.
            throw new DataCorruptionException(String.format("No operation is allowed for a sealed segment; received '%s' .", operation));
        }

        // Verify operation offset against the lastAddedOffset (whether the last Op in the list or StorageLength).
        long offset = operation.getStreamSegmentOffset();
        long length = operation.getLength();
        Preconditions.checkArgument(offset >= 0, "Operation '%s' has an invalid offset (%s).", operation, operation.getStreamSegmentOffset());
        Preconditions.checkArgument(length >= 0, "Operation '%s' has an invalid length (%s).", operation, operation.getLength());

        // Check that operations are contiguous (only for the operations after the first one - as we initialize lastAddedOffset on the first op).
        if (this.lastAddedOffset >= 0 && offset != this.lastAddedOffset) {
            throw new DataCorruptionException(String.format("Wrong offset for Operation '%s'. Expected: %d, actual: %d.", operation, this.lastAddedOffset, offset));
        }

        // Even though the DurableLog should take care of this, doesn't hurt to check again that we cannot add anything
        // after a StreamSegmentSealOperation.
        if (hasSeal) {
            throw new DataCorruptionException(String.format("Cannot add any operation after sealing a Segment; received '%s'.", operation));
        }

        // Check that the operation does not exceed the DurableLogLength of the StreamSegment.
        if (offset + length > this.metadata.getDurableLogLength()) {
            throw new DataCorruptionException(String.format(
                    "Operation '%s' has at least one byte beyond its DurableLogLength. Offset = %d, Length = %d, DurableLogLength = %d.",
                    operation,
                    offset,
                    length,
                    this.metadata.getDurableLogLength()));
        }

        if (operation instanceof StreamSegmentSealOperation) {
            // For StreamSegmentSealOperations, we must ensure the offset of the operation is equal to the DurableLogLength for the segment.
            if (this.metadata.getDurableLogLength() != offset) {
                throw new DataCorruptionException(String.format(
                        "Wrong offset for Operation '%s'. Expected: %d (DurableLogLength), actual: %d.",
                        operation,
                        this.metadata.getDurableLogLength(),
                        offset));
            }

            // Even though not an offset, we should still verify that the metadata actually thinks this is a sealed segment.
            if (!this.metadata.isSealed()) {
                throw new DataCorruptionException(String.format("Received Operation '%s' for a non-sealed segment.", operation));
            }
        }
    }

    /**
     * Updates the metadata and the internal state after a flush was completed.
     *
     * @param flushArgs The arguments used for flushing.
     * @return A FlushResult containing statistics about the flush operation.
     */
    private FlushResult updateStatePostFlush(FlushArgs flushArgs) {
        // Update the metadata Storage Length.
        long newLength = this.metadata.getStorageLength() + flushArgs.getTotalLength();
        this.metadata.setStorageLength(newLength);

        // Remove operations from the outstanding list as long as every single byte it contains has been committed.
        boolean reachedEnd = false;
        while (this.operations.size() > 0 && !reachedEnd) {
            StorageOperation first = this.operations.peek();
            long lastOffset = first.getLastStreamSegmentOffset();
            reachedEnd = lastOffset >= newLength;

            // Verify that if we did reach the 'newLength' offset, we were on an append operation. Anything else is indicative of a bug.
            assert reachedEnd || isAppendOperation(first) : "Flushed operation was not an Append.";
            if (lastOffset <= newLength) {
                this.operations.poll();
            }
        }

        // Update the outstanding length.
        long newOutstandingLength = this.outstandingAppendLength.addAndGet(-flushArgs.getTotalLength());
        assert newOutstandingLength >= 0 : "negative outstandingAppendLength";

        // Update the last flush checkpoint.
        this.lastFlush = this.stopwatch.elapsed();
        return new FlushResult().withFlushedBytes(flushArgs.getTotalLength());
    }

    /**
     * Updates the metadata and based on the given SegmentProperties object.
     *
     * @param segmentProperties The SegmentProperties object to update from.
     */
    private void updateMetadata(SegmentProperties segmentProperties) {
        this.metadata.setStorageLength(segmentProperties.getLength());
        if (segmentProperties.isSealed() && !this.metadata.isSealedInStorage()) {
            this.metadata.markSealed();
            this.metadata.markSealedInStorage();
        }
    }

    private void updateMetadataForBatchPostMerger(UpdateableSegmentMetadata batchMetadata) {
        // The other StreamSegment no longer exists and/or is no longer usable. Make sure it is marked as deleted.
        batchMetadata.markDeleted();
        this.dataSource.deleteStreamSegment(batchMetadata.getName()); // This may be redundant...

        // Complete the merger (in the ReadIndex and whatever other listeners we might have).
        this.dataSource.completeMerge(batchMetadata.getParentId(), batchMetadata.getId());
    }

    /**
     * Determines if the given StorageOperation is an Append Operation.
     *
     * @param op The operation to test.
     * @return True if an Append Operation (Cached or non-cached), false otherwise.
     */
    private boolean isAppendOperation(StorageOperation op) {
        return (op instanceof StreamSegmentAppendOperation) || (op instanceof CachedStreamSegmentAppendOperation);
    }

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.isInitialized, "SegmentAggregator is not initialized. Cannot execute this operation.");
    }

    //endregion
}
