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
import com.emc.pravega.service.contracts.RuntimeStreamingException;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.DataCorruptionException;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.UpdateableSegmentMetadata;
import com.emc.pravega.service.server.logs.operations.MergeBatchOperation;
import com.emc.pravega.service.server.logs.operations.Operation;
import com.emc.pravega.service.server.logs.operations.StorageOperation;
import com.emc.pravega.service.server.logs.operations.StreamSegmentSealOperation;
import com.emc.pravega.service.storage.Storage;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Aggregates contents for a specific StreamSegment.
 */
@Slf4j
class SegmentAggregator implements AutoCloseable {
    //region Members

    private final UpdateableSegmentMetadata metadata;
    private final WriterConfig config;
    private final LinkedList<StorageOperation> operations;
    private final AutoStopwatch stopwatch;
    private final String traceObjectId;
    private Duration lastFlush;
    private long outstandingLength;
    private long lastAddedOffset;
    private int mergeBatchCount;
    private boolean closed;

    //endregion

    //region Constructor

    SegmentAggregator(UpdateableSegmentMetadata metadata, WriterConfig config, AutoStopwatch stopwatch) {
        Preconditions.checkNotNull(metadata, "metadata");
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(stopwatch, "stopwatch");

        this.metadata = metadata;
        this.config = config;
        this.stopwatch = stopwatch;
        this.lastFlush = stopwatch.elapsed();
        this.outstandingLength = 0;
        this.lastAddedOffset = -1; // Will be set properly in initialize().
        this.mergeBatchCount = 0;
        this.operations = new LinkedList<>();
        this.traceObjectId = String.format("StorageWriter[%d-%d]", metadata.getContainerId(), metadata.getId());
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
     * Gets the SequenceNumber of the first operation that is not fully committed to Storage.
     *
     * @return The result.
     */
    long getLowestUncommittedSequenceNumber() {
        Operation firstOp = this.operations.getFirst();
        return firstOp == null ? Operation.NO_SEQUENCE_NUMBER : firstOp.getSequenceNumber();
    }

    /**
     * Gets a value representing the total length of the outstanding data in this SegmentAggregator, counting ONLY
     * StreamSegmentAppendOperations and CachedStreamSegmentAppendOperations.
     * All other operations (StreamSegmentSealOperations or MergeBatchOperations) are not counted here.
     *
     * @return The result.
     */
    long getOutstandingLength() {
        return this.outstandingLength;
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
        return this.outstandingLength >= this.config.getFlushThresholdBytes()
                || getElapsedSinceLastFlush().compareTo(this.config.getFlushThresholdTime()) >= 0
                || hasSealPending();
    }

    /**
     * Gets a value indicating whether this SegmentAggregator is ready for a merge (whether the first outstanding operation
     * is a MergeBatchOperation).
     *
     * @return The result.
     */
    boolean canMerge() {
        return hasMergePending() && this.operations.getFirst() instanceof MergeBatchOperation;
    }

    /**
     * Gets a value indicating whether this SegmentAggregator can merge with the given Segment Aggregator. This can happen
     * only if all the following conditions are met.
     * <ul>
     * <li> This SegmentAggregator refers to a stand-alone (not batch) StreamSegment that is stand-alone.
     * <li> The given SegmentAggregator refers to a batch StreamSegment having this aggregator's Segment as a parent.
     * <li> This SegmentAggregator has its first outstanding operation a MergeBatchOperation with the given aggregator's Segment.
     * </ul>
     *
     * @param aggregator The aggregator to investigate.
     * @return True if ready to merge with the other SegmentAggregator, false otherwise.
     */
    boolean canMergeWith(SegmentAggregator aggregator) {
        return this.metadata.getParentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID    // We are not a batch.
                && aggregator.getMetadata().getParentId() == this.metadata.getId()      // Given aggregator is a batch of ours.
                && canMerge()                                                           // We are ready to merge with someone.
                && ((MergeBatchOperation) this.operations.getFirst()).getBatchStreamSegmentId() == aggregator.getMetadata().getId(); // Our first op is a merge with given segment.
    }

    /**
     * Gets a value indicating whether the SegmentAggregator contains an operation that requires sealing the StreamSegment.
     * This would only be true if the last operation currently in the SegmentAggregator is a StreamSegmentSealOperation
     * or if we have a MergeBatchOperation somewhere in our pending operations.
     *
     * @return The result.
     */
    private boolean hasSealPending() {
        if (hasMergePending()) {
            return true;
        }

        Operation lastOp = this.operations.getLast();
        return lastOp != null && (lastOp instanceof StreamSegmentSealOperation);
    }

    /**
     * Gets a value indicating whether a call to merge() is required given the current state of this SegmentAggregator.
     * This would only be true if the very next operation to be flushed is a MergeBatchOperation.
     *
     * @return The result.
     */
    private boolean hasMergePending() {
        return this.mergeBatchCount > 0;
    }

    @Override
    public String toString() {
        return String.format(
                "[%d: %s] Count = %d, Length = %d, LastOffset = %d, LastFlush = %ds",
                this.metadata.getId(),
                this.metadata.getName(),
                this.operations.size(),
                this.outstandingLength,
                this.lastAddedOffset,
                this.getElapsedSinceLastFlush().toMillis() / 1000);
    }

    //endregion

    //region Operations

    /**
     * Initializes the SegmentAggregator by pulling information from the given Storage.
     *
     * @param storage The storage to initialize from.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will indicate that the operation finished successfully. If any
     * errors occurred during the operation, the Future will be completed with the appropriate exception.
     */
    CompletableFuture<Void> initialize(Storage storage, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.lastAddedOffset < 0, "SegmentAggregator has already been initialized.");

        return storage
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
                    if (!this.metadata.isSealed() && segmentInfo.isSealed()) {
                        throw new RuntimeStreamingException(new DataCorruptionException(String.format("Segment '%s' is sealed in Storage but not in the metadata.", this.metadata.getName())));
                    }

                    this.lastAddedOffset = this.metadata.getStorageLength();
                    log.info("{}: Initialized. StorageLength = {}, Sealed = {}.", this.traceObjectId, segmentInfo.getLength(), segmentInfo.isSealed());
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
        boolean isMergeBatch = operation instanceof MergeBatchOperation;
        checkSegmentId(operation, isMergeBatch);

        // Verify operation offset (this also takes care of extra operations after Seal or Merge; no need for further checks).
        boolean isMergeBatchForThisSegment = isMergeBatch && ((MergeBatchOperation) operation).getBatchStreamSegmentId() == this.metadata.getId();
        checkOffset(operation, isMergeBatchForThisSegment);

        // Add operation to list
        this.operations.addLast(operation);
        if (isMergeBatch) {
            this.mergeBatchCount++;
        }

        // Update current state (note that MergeBatchOperations have a length of 0 if added to the BatchStreamSegment - because they don't have any effect on it).
        long operationLength = isMergeBatchForThisSegment ? 0 : operation.getLength();
        this.outstandingLength += operationLength;
        this.lastAddedOffset = operation.getStreamSegmentOffset() + operationLength;
    }

    /**
     * Flushes the contents of the Aggregator to the given Storage.
     *
     * @param storage The Storage to flush the contents to.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a summary of the flush operation. If any errors
     * occurred during the flush, the Future will be completed with the appropriate exception.
     */
    CompletableFuture<FlushResult> flush(Storage storage, Duration timeout) {
        ensureInitializedAndNotClosed();

        // TODO: implement flush.

        this.lastFlush = this.stopwatch.elapsed();
        return CompletableFuture.completedFuture(new FlushResult(0));
    }

    /**
     * Merges the given SegmentAggregator into this one.
     * Preconditions:
     * <ul>
     * <li> This StreamSegment is stand-alone (not a batch).
     * <li> Given Segment is a batch of this StreamSegment.
     * <li> Both this StreamSegment and the given StreamSegment are fully flushed up until the point of the merger (i.e., the merger is the next operation to process).
     * </ul>
     * Effects of the merger:
     * <ul> The entire contents of the given batch StreamSegment will be concatenated to this StreamSegment as one unit.
     * <li> The metadata for this StreamSegment will be updated to reflect the new length of this StreamSegment.
     * <li> The given batch SegmentAggregator will be closed (its metadata will not be touched by this method).
     * </ul>
     * <p>
     * Note that various other data integrity checks are done pre and post merger as part of this operation which are meant
     * to ensure the StreamSegment is not in a corrupted state.
     *
     * @param batchAggregator The SegmentAggregator to merge.
     * @param storage         The Storage to execute the operation in.
     * @param timeout         Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the number of bytes that were merged into this
     * StreamSegment. If failed, the Future will contain the exception that caused it.
     */
    CompletableFuture<FlushResult> mergeWith(SegmentAggregator batchAggregator, Storage storage, Duration timeout) {
        ensureInitializedAndNotClosed();

        // Verify we can actually merge with this Segment (canMergeWith has all the conditions required for that).
        Preconditions.checkArgument(canMergeWith(batchAggregator), "Cannot merge given SegmentAggregator into this one.");

        // Verify the given Batch StreamSegment's Metadata indicates it's fully flushed to Storage.
        SegmentMetadata batchMetadata = batchAggregator.getMetadata();
        Preconditions.checkArgument(
                batchMetadata.getDurableLogLength() == batchMetadata.getStorageLength(),
                "Segment %s cannot be merged into %s because it is not fully flushed. DurableLogLength=%s, StorageLength=%s",
                batchMetadata.getName(), this.metadata.getName(), batchMetadata.getDurableLogLength(), batchMetadata.getStorageLength());

        AtomicLong mergedLength = new AtomicLong();
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return storage
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
                .thenCompose(v1 -> storage.concat(this.metadata.getName(), batchAggregator.getMetadata().getName(), timer.getRemaining()))
                .thenCompose(v2 -> storage.getStreamSegmentInfo(this.metadata.getName(), timer.getRemaining()))
                .thenApply(segmentProperties -> {
                    // We have processed a MergeBatchOperation, pop the first operation off and decrement the counter.
                    StorageOperation processedOperation = this.operations.removeFirst();
                    assert processedOperation instanceof MergeBatchOperation : "First outstanding operation was not a MergeBatchOperation";
                    this.mergeBatchCount--;
                    assert this.mergeBatchCount >= 0 : "Negative value for mergeBatchCount";

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

                    // Update our metadata with the new length.
                    updateMetadata(segmentProperties);

                    // The other StreamSegment no longer exists and/or is no longer usable. Make sure it is closed.
                    // The owning StorageWriter will do the actual disposal of it and Metadata cleanup.
                    batchAggregator.close();
                    this.lastFlush = this.stopwatch.elapsed();
                    return new FlushResult(mergedLength.get());
                });
    }

    //endregion

    //region Helpers

    /**
     * Ensures the following conditions are met:
     * * Regular Operations: SegmentId matches this SegmentAggregator's SegmentId
     * * Batches: TargetSegmentId/SegmentId matches this SegmentAggregator's SegmentId.
     *
     * @param operation    The operation to check.
     * @param isMergeBatch Whether the given operation is a MergeBatchOperation.
     * @throws IllegalArgumentException If any of the validations failed.
     */
    private void checkSegmentId(StorageOperation operation, boolean isMergeBatch) {
        // All exceptions thrown from here are RuntimeExceptions (as opposed from DataCorruptionExceptions); they are indicative
        // of bad code (objects got routed to wrong SegmentAggregators) and not data corruption.
        if (isMergeBatch) {
            MergeBatchOperation mbo = (MergeBatchOperation) operation;
            if (this.metadata.getParentId() != ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                // We are a batch StreamSegment; verify that the Operation has us as a batch to merge.
                Preconditions.checkArgument(
                        mbo.getBatchStreamSegmentId() == this.metadata.getId(),
                        "Operation '%s' refers to a different StreamSegment than this one (%s).", operation, this.metadata.getId());
            } else {
                // We are a stand-alone StreamSegment; verify that the Operation has us as a parent (target).
                Preconditions.checkArgument(
                        mbo.getStreamSegmentId() == this.metadata.getId(),
                        "Operation '%s' refers to a different StreamSegment as a target than this one (%s).", operation, this.metadata.getId());
            }
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
     * @param operation                  The operation to check.
     * @param isMergeBatchForThisSegment Whether the given operation is a Batch for this Segment (as opposed to it being
     *                                   for a child segment).
     * @throws DataCorruptionException  If any of the validations failed.
     * @throws IllegalArgumentException If the operation has an undefined Offset or Length (these are not considered data-
     *                                  corrupting issues).
     */
    private void checkOffset(StorageOperation operation, boolean isMergeBatchForThisSegment) throws DataCorruptionException {
        // Verify operation offset against the lastAddedOffset (whether the last Op in the list or StorageLength).
        long offset = operation.getStreamSegmentOffset();
        long length = operation.getLength();
        Preconditions.checkArgument(offset >= 0, "Operation '%s' has an invalid offset (%s).", operation, operation.getStreamSegmentOffset());
        Preconditions.checkArgument(length >= 0, "Operation '%s' has an invalid length (%s).", operation, operation.getLength());

        if (!isMergeBatchForThisSegment) {
            // Check that operations are contiguous.
            if (offset != this.lastAddedOffset) {
                throw new DataCorruptionException(String.format("Wrong offset for Operation '%s'. Expected: %d, actual: %d.", operation, this.lastAddedOffset, offset));
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
        }

        boolean requiresSealedSegment = false;
        if (operation instanceof StreamSegmentSealOperation) {
            // For StreamSegmentSealOperations, we must ensure the offset of the operation is equal to the DurableLogLength for the segment.
            if (this.metadata.getDurableLogLength() != offset) {
                throw new DataCorruptionException(String.format(
                        "Wrong offset for Operation '%s'. Expected: %d (DurableLogLength), actual: %d.",
                        operation,
                        this.metadata.getDurableLogLength(),
                        offset));
            }

            requiresSealedSegment = true;
            // Even though not an offset, we should still verify that the metadata actually thinks this is a sealed segment.
            if (!this.metadata.isSealed()) {
                throw new DataCorruptionException(String.format("Received Operation '%s' for a non-sealed segment.", operation));
            }
        } else if (isMergeBatchForThisSegment) {
            // Only need to do the check for MergeBatchOperation as batch StreamSegments (the only thing needed as parents is the offset & length check).
            // We must ensure the BatchStreamSegmentLength of the operation is equal to the DurableLogLength for the segment.
            if (this.metadata.getDurableLogLength() != length) {
                throw new DataCorruptionException(String.format(
                        "Wrong BatchStreamSegmentLength for Operation '%s'. Expected: %d (DurableLogLength), actual: %d.",
                        operation,
                        this.metadata.getDurableLogLength(),
                        length));
            }

            requiresSealedSegment = true;
        }

        if (requiresSealedSegment) {
            // Even though not an offset, we should still verify that the metadata actually thinks this is a sealed segment.
            if (!this.metadata.isSealed()) {
                throw new DataCorruptionException(String.format("Received Operation '%s' for a non-sealed segment.", operation));
            }
        }
    }

    private void updateMetadata(SegmentProperties segmentProperties) {
        this.metadata.setStorageLength(segmentProperties.getLength());
        if (segmentProperties.isSealed() && !this.metadata.isSealed()) {
            this.metadata.markSealed();
        }
    }

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.lastAddedOffset >= 0, "SegmentAggregator is not initialized. Cannot execute this operation.");
    }

    //endregion
}
