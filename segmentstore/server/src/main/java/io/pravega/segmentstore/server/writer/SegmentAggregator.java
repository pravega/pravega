/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.writer;

import com.google.common.base.Preconditions;
import io.pravega.common.AbstractTimer;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.logs.SerializationException;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.MergeTransactionOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.OperationType;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Aggregates contents for a specific StreamSegment.
 */
@Slf4j
class SegmentAggregator implements OperationProcessor, AutoCloseable {
    //region Members

    private final UpdateableSegmentMetadata metadata;
    private final WriterConfig config;
    private final OperationQueue operations;
    private final AbstractTimer timer;
    private final String traceObjectId;
    private final Storage storage;
    private final AtomicReference<SegmentHandle> handle;
    private final WriterDataSource dataSource;
    private final AtomicInteger mergeTransactionCount;
    private final AtomicInteger truncateCount;
    private final AtomicBoolean hasSealPending;
    private final AtomicLong lastAddedOffset;
    private final AtomicReference<Duration> lastFlush;
    private final AtomicReference<AggregatorState> state;
    private final AtomicReference<ReconciliationState> reconciliationState;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the SegmentAggregator class.
     *
     * @param segmentMetadata The Metadata for the StreamSegment to construct this Aggregator for.
     * @param dataSource      The WriterDataSource to use.
     * @param storage         The Storage to use (for flushing).
     * @param config          The Configuration to use.
     * @param timer           A Timer to use to determine elapsed time.
     */
    SegmentAggregator(UpdateableSegmentMetadata segmentMetadata, WriterDataSource dataSource, Storage storage, WriterConfig config, AbstractTimer timer) {
        Preconditions.checkNotNull(segmentMetadata, "segmentMetadata");
        Preconditions.checkNotNull(dataSource, "dataSource");
        Preconditions.checkNotNull(storage, "storage");
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(timer, "timer");

        this.metadata = segmentMetadata;
        Preconditions.checkArgument(this.metadata.getContainerId() == dataSource.getId(), "SegmentMetadata.ContainerId is different from WriterDataSource.Id");
        this.traceObjectId = String.format("StorageWriter[%d-%d]", this.metadata.getContainerId(), this.metadata.getId());

        this.config = config;
        this.storage = storage;
        this.dataSource = dataSource;
        this.timer = timer;
        this.lastFlush = new AtomicReference<>(timer.getElapsed());
        this.lastAddedOffset = new AtomicLong(-1); // Will be set properly in initialize().
        this.mergeTransactionCount = new AtomicInteger();
        this.truncateCount = new AtomicInteger();
        this.hasSealPending = new AtomicBoolean();
        this.operations = new OperationQueue();
        this.state = new AtomicReference<>(AggregatorState.NotInitialized);
        this.reconciliationState = new AtomicReference<>();
        this.handle = new AtomicReference<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!isClosed()) {
            setState(AggregatorState.Closed);
        }
    }

    //endregion

    //region OperationProcessor Implementation

    @Override
    public long getLowestUncommittedSequenceNumber() {
        StorageOperation first = this.operations.getFirst();
        return first == null ? Operation.NO_SEQUENCE_NUMBER : first.getSequenceNumber();
    }

    @Override
    public boolean isClosed() {
        return this.state.get() == AggregatorState.Closed;
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
     */
    Duration getElapsedSinceLastFlush() {
        return this.timer.getElapsed().minus(this.lastFlush.get());
    }

    /**
     * Gets a value indicating whether a call to flush() is required given the current state of this SegmentAggregator.
     * <p>
     * Any of the following conditions can trigger a flush:
     * <ul>
     * <li> There is more data in the SegmentAggregator than the configuration allows (getOutstandingLength >= FlushThresholdBytes)
     * <li> Too much time has passed since the last call to flush() (getElapsedSinceLastFlush >= FlushThresholdTime)
     * <li> The SegmentAggregator contains a StreamSegmentSealOperation or MergeTransactionOperation (hasSealPending == true)
     * <li> The SegmentAggregator is currently in a Reconciliation State (recovering from an inconsistency in Storage).
     * </ul>
     */
    boolean mustFlush() {
        if (this.metadata.isDeleted()) {
            return false;
        }

        return exceedsThresholds()
                || this.hasSealPending.get()
                || this.mergeTransactionCount.get() > 0
                || this.truncateCount.get() > 0
                || (this.operations.size() > 0 && isReconciling());
    }

    /**
     * Gets a value indicating whether the Flush thresholds are exceeded for this SegmentAggregator.
     */
    private boolean exceedsThresholds() {
        long length = this.operations.size() > 0 && isAppendOperation(this.operations.getFirst()) ?
                this.operations.getFirst().getLength() :
                0;
        return length >= this.config.getFlushThresholdBytes()
                || (length > 0 && getElapsedSinceLastFlush().compareTo(this.config.getFlushThresholdTime()) >= 0);
    }

    /**
     * Gets a value indicating whether the SegmentAggregator is currently in a Reconciliation state.
     */
    private boolean isReconciling() {
        AggregatorState currentState = this.state.get();
        return currentState == AggregatorState.ReconciliationNeeded
                || currentState == AggregatorState.Reconciling;
    }

    @Override
    public String toString() {
        return String.format(
                "[%d: %s] Count = %d, LastOffset = %s, LUSN = %d LastFlush = %ds",
                this.metadata.getId(),
                this.metadata.getName(),
                this.operations.size(),
                this.lastAddedOffset,
                getLowestUncommittedSequenceNumber(),
                this.getElapsedSinceLastFlush().toMillis() / 1000);
    }

    //endregion

    //region Operations

    /**
     * Initializes the SegmentAggregator by pulling information from the given Storage.
     *
     * @param timeout  Timeout for the operation.
     * @param executor An executor to execute async tasks on.
     * @return A CompletableFuture that, when completed, will indicate that the operation finished successfully. If any
     * errors occurred during the operation, the Future will be completed with the appropriate exception.
     */
    CompletableFuture<Void> initialize(Duration timeout, Executor executor) {
        Exceptions.checkNotClosed(isClosed(), this);
        Preconditions.checkState(this.state.get() == AggregatorState.NotInitialized, "SegmentAggregator has already been initialized.");
        assert this.handle.get() == null : "non-null handle but state == " + this.state.get();
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "initialize");

        return openWrite(this.metadata.getName(), this.handle, executor, timeout)
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
                            throw new CompletionException(new DataCorruptionException(String.format("Segment '%s' is sealed in Storage but not in the metadata.", this.metadata.getName())));
                        }

                        if (!this.metadata.isSealedInStorage()) {
                            this.metadata.markSealedInStorage();
                            log.warn("{}: Segment is sealed in Storage but metadata does not reflect that - updating metadata.", this.traceObjectId, segmentInfo.getLength());
                        }
                    }

                    log.info("{}: Initialized. StorageLength = {}, Sealed = {}.", this.traceObjectId, segmentInfo.getLength(), segmentInfo.isSealed());
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "initialize", traceId);
                    setState(AggregatorState.Writing);
                })
                .exceptionally(ex -> {
                    ex = Exceptions.unwrap(ex);
                    if (ex instanceof StreamSegmentNotExistsException) {
                        // Segment does not exist anymore. This is a real possibility during recovery, in the following cases:
                        // * We already processed a Segment Deletion but did not have a chance to checkpoint metadata
                        // * We processed a TransactionMergeOperation but did not have a chance to ack/truncate the DataSource
                        this.metadata.markDeleted(); // Update metadata, just in case it is not already updated.
                        log.warn("{}: Segment does not exist in Storage. Ignoring all further operations on it.", this.traceObjectId, ex);
                        setState(AggregatorState.Writing);
                        LoggerHelpers.traceLeave(log, this.traceObjectId, "initialize", traceId);
                    } else {
                        // Other kind of error - re-throw.
                        throw new CompletionException(ex);
                    }

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
        boolean isTruncate = isTruncateOperation(operation);
        boolean processOp = lastOffset > this.metadata.getStorageLength()
                || isTruncate
                || (!this.metadata.isSealedInStorage() && (operation instanceof StreamSegmentSealOperation));
        if (processOp) {
            processNewOperation(operation);
        } else {
            acknowledgeAlreadyProcessedOperation(operation);
        }

        if (!isTruncate) {
            // Always record the last added offset, to ensure that operations are contiguous and processed in the right order.
            this.lastAddedOffset.set(lastOffset);
        }
        log.debug("{}: Add {}; OpCount={}, MergeCount={}, Seal={}.", this.traceObjectId, operation, this.operations.size(), this.mergeTransactionCount, this.hasSealPending);
    }

    /**
     * Processes an operation that would result in a change to the underlying Storage.
     *
     * @param operation The Operation to process.
     */
    private void processNewOperation(StorageOperation operation) {
        if (operation instanceof MergeTransactionOperation) {
            this.operations.add(operation);
            this.mergeTransactionCount.incrementAndGet();
        } else if (operation instanceof StreamSegmentSealOperation) {
            this.operations.add(operation);
            this.hasSealPending.set(true);
        } else if (operation instanceof StreamSegmentTruncateOperation) {
            this.operations.add(operation);
            this.truncateCount.incrementAndGet();
        } else if (operation instanceof CachedStreamSegmentAppendOperation) {
            // Aggregate the Append Operation.
            final int maxLength = this.config.getMaxFlushSizeBytes();
            AggregatedAppendOperation aggregatedAppend = getOrCreateAggregatedAppend(
                    operation.getStreamSegmentOffset(), operation.getSequenceNumber(), maxLength);
            aggregateAppendOperation(operation, aggregatedAppend, maxLength);
        }
    }

    /**
     * Processes (acks) an operation that has already been processed and would otherwise result in no change to the
     * underlying Storage.
     *
     * @param operation The operation to handle.
     */
    private void acknowledgeAlreadyProcessedOperation(StorageOperation operation) {
        try {
            // Only MergeTransactionOperations need special handling. Others, such as StreamSegmentSealOperation, are not
            // needed since they're handled in the initialize() method.
            if (operation instanceof MergeTransactionOperation) {
                // Ensure that the DataSource is aware of this (since after recovery, it may not know that a merge has
                // been properly completed).
                MergeTransactionOperation mergeOp = (MergeTransactionOperation) operation;
                this.dataSource.completeMerge(mergeOp.getStreamSegmentId(), mergeOp.getTransactionSegmentId());
            }
        } catch (Exception ex) {
            log.warn("Unable to acknowledge already processed operation '{}'.", operation, ex);
        }
    }

    /**
     * Aggregates the given operation by recording the fact that it exists in the current Aggregated Append.
     * An Aggregated Append cannot exceed a certain size, so if the given operation doesn't fit, it will need to
     * span multiple AggregatedAppends.
     * When aggregating, the SequenceNumber of the AggregatedAppend is the sequence number of the operation
     * that occupies its first byte. As such, getLowestUncommittedSequenceNumber() will always return the SeqNo
     * of that first operation that hasn't yet been fully flushed.
     *
     * @param operation        The operation to aggregate.
     * @param aggregatedAppend The AggregatedAppend to add to.
     * @param maxLength        The maximum length that the Aggregated Append can have.
     */
    private void aggregateAppendOperation(StorageOperation operation, AggregatedAppendOperation aggregatedAppend, int maxLength) {
        long remainingLength = operation.getLength();
        while (remainingLength > 0) {
            // All append lengths are integers, so it's safe to cast here.
            int lengthToAdd = (int) Math.min(maxLength - aggregatedAppend.getLength(), remainingLength);
            aggregatedAppend.increaseLength(lengthToAdd);
            remainingLength -= lengthToAdd;
            if (remainingLength > 0) {
                // We still have data to add, which means we filled up the current AggregatedAppend; make a new one.
                assert aggregatedAppend.getLength() == maxLength : "Unexpected AggregatedAppend.length when there is data to add";
                aggregatedAppend = new AggregatedAppendOperation(this.metadata.getId(), aggregatedAppend.getLastStreamSegmentOffset(), operation.getSequenceNumber());
                this.operations.add(aggregatedAppend);
            }
        }
    }

    /**
     * Gets an existing AggregatedAppendOperation or creates a new one to add the given operation to.
     * An existing AggregatedAppend will be returned if it meets the following criteria:
     * * It is the last operation in the operation queue.
     * * Its size is smaller than maxLength.
     * * It is not sealed (already flushed).
     * <p>
     * If at least one of the above criteria is not met, a new AggregatedAppend is created and added at the end of the
     * operation queue.
     *
     * @param operationOffset         The Segment Offset of the operation to add.
     * @param operationSequenceNumber The Sequence Number of the operation to add.
     * @param maxLength               The maximum length for an aggregated append.
     * @return The AggregatedAppend to use (existing or freshly created).
     */
    private AggregatedAppendOperation getOrCreateAggregatedAppend(long operationOffset, long operationSequenceNumber, int maxLength) {
        AggregatedAppendOperation aggregatedAppend = null;
        if (this.operations.size() > 0) {
            StorageOperation last = this.operations.getLast();
            if (last.getLength() < maxLength && isAppendOperation(last)) {
                aggregatedAppend = (AggregatedAppendOperation) last;
                if (aggregatedAppend.isSealed()) {
                    aggregatedAppend = null;
                }
            }
        }

        if (aggregatedAppend == null) {
            // No operations or last operation not an AggregatedAppend - create a new one.
            aggregatedAppend = new AggregatedAppendOperation(this.metadata.getId(), operationOffset, operationSequenceNumber);
            this.operations.add(aggregatedAppend);
        }

        return aggregatedAppend;
    }

    //endregion

    //region Flushing and Merging

    /**
     * Flushes the contents of the Aggregator to the Storage.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a summary of the flush operation. If any errors
     * occurred during the flush, the Future will be completed with the appropriate exception.
     */
    CompletableFuture<FlushResult> flush(Duration timeout, Executor executor) {
        ensureInitializedAndNotClosed();
        if (this.metadata.isDeleted()) {
            // Segment has been deleted; don't do anything else.
            return CompletableFuture.completedFuture(new FlushResult());
        }

        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "flush");

        TimeoutTimer timer = new TimeoutTimer(timeout);
        CompletableFuture<FlushResult> result;
        try {
            switch (this.state.get()) {
                case Writing:
                    result = flushNormally(timer, executor);
                    break;
                case ReconciliationNeeded:
                    result = beginReconciliation(timer)
                            .thenComposeAsync(v -> reconcile(timer, executor), executor);
                    break;
                case Reconciling:
                    result = reconcile(timer, executor);
                    break;
                //$CASES-OMITTED$
                default:
                    result = Futures.failedFuture(new IllegalStateException(String.format("Unexpected state for SegmentAggregator (%s) for segment '%s'.", this.state, this.metadata.getName())));
                    break;
            }
        } catch (Exception ex) {
            // Convert synchronous errors into async errors - it's easier to handle on the receiving end.
            result = Futures.failedFuture(ex);
        }

        return result
                .thenApply(r -> {
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "flush", traceId, r);
                    return r;
                });
    }

    /**
     * Repeatedly flushes the contents of the Aggregator to the Storage as long as something immediate needs to be flushed,
     * such as a Seal or Merge operation.
     *
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation.
     */
    private CompletableFuture<FlushResult> flushNormally(TimeoutTimer timer, Executor executor) {
        assert this.state.get() == AggregatorState.Writing : "flushNormally cannot be called if state == " + this.state;
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "flushNormally", this.operations.size());
        FlushResult result = new FlushResult();
        AtomicBoolean canContinue = new AtomicBoolean(true);
        return Futures
                .loop(
                        canContinue::get,
                        () -> flushOnce(timer, executor),
                        partialResult -> {
                            canContinue.set(partialResult.getFlushedBytes() + partialResult.getMergedBytes() > 0);
                            result.withFlushResult(partialResult);
                        },
                        executor)
                .thenApply(v -> {
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "flushNormally", traceId, result);
                    return result;
                });
    }

    /**
     * Flushes the contents of the Aggregator exactly once to the Storage in a 'normal' mode (where it does not need to
     * do any reconciliation).
     *
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation.
     */
    private CompletableFuture<FlushResult> flushOnce(TimeoutTimer timer, Executor executor) {
        boolean hasMerge = this.mergeTransactionCount.get() > 0;
        boolean hasSeal = this.hasSealPending.get();
        boolean hasTruncate = this.truncateCount.get() > 0;
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "flushOnce", this.operations.size(),
                this.mergeTransactionCount, hasSeal, hasTruncate);

        CompletableFuture<FlushResult> result;
        if (hasSeal || hasMerge || hasTruncate) {
            // If we have a Seal or Merge Pending, flush everything until we reach that operation.
            result = flushFully(timer, executor);
            if (hasMerge) {
                // If we have a merge, do it after we flush fully.
                result = result.thenComposeAsync(flushResult -> mergeIfNecessary(flushResult, timer, executor), executor);
            }

            if (hasSeal) {
                // If we have a seal, do it after every other operation.
                result = result.thenComposeAsync(flushResult -> sealIfNecessary(flushResult, timer), executor);
            }
        } else {
            // Otherwise, just flush the excess as long as we have something to flush.
            result = flushExcess(timer, executor);
        }

        return result
                .thenApply(r -> {
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "flushOnce", traceId, r);
                    return r;
                });
    }

    /**
     * Flushes all Append Operations that can be flushed at the given moment (until the entire Aggregator is emptied out
     * or until a StreamSegmentSealOperation or MergeTransactionOperation is encountered).
     *
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation.
     */
    private CompletableFuture<FlushResult> flushFully(TimeoutTimer timer, Executor executor) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "flushFully");
        FlushResult result = new FlushResult();
        return Futures
                .loop(
                        this::canContinueFlushingFully,
                        () -> flushPendingAppends(timer.getRemaining())
                                .thenCompose(flushResult -> flushPendingTruncate(flushResult, timer.getRemaining())),
                        result::withFlushResult,
                        executor)
                .thenApply(v -> {
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "flushFully", traceId, result);
                    return result;
                });
    }

    /**
     * Determines whether flushFully can continue given the current state of this SegmentAggregator.
     */
    private boolean canContinueFlushingFully() {
        StorageOperation next = this.operations.getFirst();
        return isAppendOperation(next) || isTruncateOperation(next);
    }

    /**
     * Flushes as many Append Operations as needed as long as the data inside this SegmentAggregator exceeds size/time thresholds.
     * This will stop when either the thresholds are not exceeded anymore or when a non-Append Operation is encountered.
     *
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation.
     */
    private CompletableFuture<FlushResult> flushExcess(TimeoutTimer timer, Executor executor) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "flushExcess");
        FlushResult result = new FlushResult();
        return Futures
                .loop(
                        this::canContinueFlushingExcess,
                        () -> flushPendingAppends(timer.getRemaining())
                                .thenCompose(flushResult -> flushPendingTruncate(flushResult, timer.getRemaining())),
                        result::withFlushResult,
                        executor)
                .thenApply(v -> {
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "flushExcess", traceId, result);
                    return result;
                });
    }

    /**
     * Determines whether flushExcess can continue given the current state of this SegmentAggregator.
     */
    private boolean canContinueFlushingExcess() {
        return !this.metadata.isDeleted() && exceedsThresholds() || isTruncateOperation(this.operations.getFirst());
    }

    /**
     * Flushes a pending StreamSegmentTruncateOperation, if that is the next pending one.
     *
     * @param flushResult The FlushResult of the operation just prior to this.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation, merged in with
     * the one passed in as input.
     */
    private CompletableFuture<FlushResult> flushPendingTruncate(FlushResult flushResult, Duration timeout) {
        StorageOperation op = this.operations.getFirst();
        if (!isTruncateOperation(op) || !this.storage.supportsTruncation()) {
            // Nothing to do.
            return CompletableFuture.completedFuture(flushResult);
        }

        long truncateOffset = Math.min(this.metadata.getStorageLength(), op.getStreamSegmentOffset());
        return this.storage.truncate(this.handle.get(), truncateOffset, timeout)
                           .thenApply(v -> {
                               updateStatePostTruncate();
                               return flushResult;
                           });
    }

    /**
     * Flushes all Append Operations that can be flushed up to the maximum allowed flush size.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation.
     */
    private CompletableFuture<FlushResult> flushPendingAppends(Duration timeout) {
        // Gather an InputStream made up of all the operations we can flush.
        FlushArgs flushArgs;
        try {
            flushArgs = getFlushArgs();
        } catch (DataCorruptionException ex) {
            return Futures.failedFuture(ex);
        }

        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "flushPendingAppends");

        if (flushArgs.getLength() == 0) {
            // Nothing to flush.
            FlushResult result = new FlushResult();
            LoggerHelpers.traceLeave(log, this.traceObjectId, "flushPendingAppends", traceId, result);
            return CompletableFuture.completedFuture(result);
        }

        // Flush them.
        InputStream inputStream = flushArgs.getStream();
        return this.storage
                .write(this.handle.get(), this.metadata.getStorageLength(), inputStream, flushArgs.getLength(), timeout)
                .thenApply(v -> {
                    FlushResult result = updateStatePostFlush(flushArgs);
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "flushPendingAppends", traceId, result);
                    return result;
                })
                .exceptionally(ex -> {
                    if (Exceptions.unwrap(ex) instanceof BadOffsetException) {
                        // We attempted to write at an offset that already contained other data. This can happen for a number of
                        // reasons, but we do not have enough information here to determine why. We need to enter reconciliation
                        // mode, and hope for the best.
                        setState(AggregatorState.ReconciliationNeeded);
                    }

                    // Rethrow all exceptions.
                    throw new CompletionException(ex);
                });
    }

    /**
     * Returns a FlushArgs which contains the data needing to be flushed to Storage.
     *
     * @return The aggregated object that can be used for flushing.
     * @throws DataCorruptionException If a unable to retrieve required data from the Data Source.
     */
    private FlushArgs getFlushArgs() throws DataCorruptionException {
        StorageOperation first = this.operations.getFirst();
        if (!(first instanceof AggregatedAppendOperation)) {
            // Nothing to flush - first operation is not an AggregatedAppend.
            return new FlushArgs(null, 0);
        }

        AggregatedAppendOperation appendOp = (AggregatedAppendOperation) first;
        int length = (int) appendOp.getLength();
        InputStream data = this.dataSource.getAppendData(appendOp.getStreamSegmentId(), appendOp.getStreamSegmentOffset(), length);
        if (data == null) {
            if (this.metadata.isDeleted()) {
                // Segment was deleted - nothing more to do.
                return new FlushArgs(null, 0);
            }
            throw new DataCorruptionException(String.format("Unable to retrieve CacheContents for '%s'.", appendOp));
        }

        appendOp.seal();
        return new FlushArgs(data, length);
    }

    /**
     * Executes a merger of a Transaction StreamSegment into this one.
     * Conditions for merger:
     * <ul>
     * <li> This StreamSegment is stand-alone (not a Transaction).
     * <li> The next outstanding operation is a MergeTransactionOperation for a Transaction StreamSegment of this StreamSegment.
     * <li> The StreamSegment to merge is not deleted, it is sealed and is fully flushed to Storage.
     * </ul>
     * Effects of the merger:
     * <ul> The entire contents of the given Transaction StreamSegment will be concatenated to this StreamSegment as one unit.
     * <li> The metadata for this StreamSegment will be updated to reflect the new length of this StreamSegment.
     * <li> The given Transaction Segment will cease to exist.
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
    private CompletableFuture<FlushResult> mergeIfNecessary(FlushResult flushResult, TimeoutTimer timer, Executor executor) {
        ensureInitializedAndNotClosed();
        assert !this.metadata.isTransaction() : "Cannot merge into a Transaction StreamSegment.";
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "mergeIfNecessary");

        StorageOperation first = this.operations.getFirst();
        if (first == null || !(first instanceof MergeTransactionOperation)) {
            // Either no operation or first operation is not a MergeTransaction. Nothing to do.
            LoggerHelpers.traceLeave(log, this.traceObjectId, "mergeIfNecessary", traceId, flushResult);
            return CompletableFuture.completedFuture(flushResult);
        }

        MergeTransactionOperation mergeTransactionOperation = (MergeTransactionOperation) first;
        UpdateableSegmentMetadata transactionMetadata = this.dataSource.getStreamSegmentMetadata(mergeTransactionOperation.getTransactionSegmentId());
        return mergeWith(transactionMetadata, mergeTransactionOperation, timer, executor)
                .thenApply(mergeResult -> {
                    flushResult.withFlushResult(mergeResult);
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "mergeIfNecessary", traceId, flushResult);
                    return flushResult;
                });
    }

    /**
     * Merges the Transaction StreamSegment with given metadata into this one at the current offset.
     *
     * @param transactionMetadata The metadata of the Transaction StreamSegment to merge.
     * @param timer               Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the number of bytes that were merged into this
     * StreamSegment. If failed, the Future will contain the exception that caused it.
     */
    private CompletableFuture<FlushResult> mergeWith(UpdateableSegmentMetadata transactionMetadata, MergeTransactionOperation mergeOp, TimeoutTimer timer, Executor executor) {
        if (transactionMetadata.isDeleted()) {
            return Futures.failedFuture(new DataCorruptionException(String.format("Attempted to merge with deleted Transaction segment '%s'.", transactionMetadata.getName())));
        }

        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "mergeWith", transactionMetadata.getId(), transactionMetadata.getName(), transactionMetadata.isSealedInStorage());
        FlushResult result = new FlushResult();
        if (!transactionMetadata.isSealedInStorage() || transactionMetadata.getLength() > transactionMetadata.getStorageLength()) {
            // Nothing to do. Given Transaction is not eligible for merger yet.
            LoggerHelpers.traceLeave(log, this.traceObjectId, "mergeWith", traceId, result);
            return CompletableFuture.completedFuture(result);
        }

        AtomicLong mergedLength = new AtomicLong();
        return this.storage
                .getStreamSegmentInfo(transactionMetadata.getName(), timer.getRemaining())
                .thenAcceptAsync(transProperties -> {
                    // One last verification before the actual merger:
                    // Check that the Storage agrees with our metadata (if not, we have a problem ...)
                    if (transProperties.getLength() != transactionMetadata.getStorageLength()) {
                        throw new CompletionException(new DataCorruptionException(String.format(
                                "Transaction Segment '%s' cannot be merged into parent '%s' because its metadata disagrees with the Storage. Metadata.StorageLength=%d, Storage.StorageLength=%d",
                                transactionMetadata.getName(),
                                this.metadata.getName(),
                                transactionMetadata.getStorageLength(),
                                transProperties.getLength())));
                    }

                    if (transProperties.getLength() != mergeOp.getLength()) {
                        throw new CompletionException(new DataCorruptionException(String.format(
                                "Transaction Segment '%s' cannot be merged into parent '%s' because the declared length in the operation disagrees with the Storage. Operation.Length=%d, Storage.StorageLength=%d",
                                transactionMetadata.getName(),
                                this.metadata.getName(),
                                mergeOp.getLength(),
                                transProperties.getLength())));
                    }

                    mergedLength.set(transProperties.getLength());
                }, executor)
                .thenComposeAsync(v1 -> storage.concat(this.handle.get(), mergeOp.getStreamSegmentOffset(), transactionMetadata.getName(), timer.getRemaining()), executor)
                .thenComposeAsync(v2 -> storage.getStreamSegmentInfo(this.metadata.getName(), timer.getRemaining()), executor)
                .thenApplyAsync(segmentProperties -> {
                    // We have processed a MergeTransactionOperation, pop the first operation off and decrement the counter.
                    StorageOperation processedOperation = this.operations.removeFirst();
                    assert processedOperation != null && processedOperation instanceof MergeTransactionOperation : "First outstanding operation was not a MergeTransactionOperation";
                    assert ((MergeTransactionOperation) processedOperation).getTransactionSegmentId() == transactionMetadata.getId() : "First outstanding operation was a MergeTransactionOperation for the wrong Transaction id.";
                    int newCount = this.mergeTransactionCount.decrementAndGet();
                    assert newCount >= 0 : "Negative value for mergeTransactionCount";

                    // Post-merger validation. Verify we are still in agreement with the storage.
                    long expectedNewLength = this.metadata.getStorageLength() + mergedLength.get();
                    if (segmentProperties.getLength() != expectedNewLength) {
                        throw new CompletionException(new DataCorruptionException(String.format(
                                "Transaction Segment '%s' was merged into parent '%s' but the parent segment has an unexpected StorageLength after the merger. Previous=%d, MergeLength=%d, Expected=%d, Actual=%d",
                                transactionMetadata.getName(),
                                this.metadata.getName(),
                                segmentProperties.getLength(),
                                mergedLength.get(),
                                expectedNewLength,
                                segmentProperties.getLength())));
                    }

                    updateMetadata(segmentProperties);
                    updateMetadataForTransactionPostMerger(transactionMetadata);

                    this.lastFlush.set(this.timer.getElapsed());
                    result.withMergedBytes(mergedLength.get());
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "mergeWith", traceId, result);
                    return result;
                }, executor)
                .exceptionally(ex -> {
                    Throwable realEx = Exceptions.unwrap(ex);
                    if (realEx instanceof BadOffsetException || realEx instanceof StreamSegmentNotExistsException) {
                        // We either attempted to write at an offset that already contained other data or the Transaction
                        // Segment no longer exists. This can happen for a number of reasons, but we do not have enough
                        // information here to determine why. We need to enter reconciliation mode, and hope for the best.
                        setState(AggregatorState.ReconciliationNeeded);
                    }

                    // Rethrow all exceptions.
                    throw new CompletionException(ex);
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
        if (!this.hasSealPending.get() || !(this.operations.getFirst() instanceof StreamSegmentSealOperation)) {
            // Either no Seal is pending or the next operation is not a seal - we cannot execute a seal.
            return CompletableFuture.completedFuture(flushResult);
        }

        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "sealIfNecessary");
        return this.storage
                .seal(this.handle.get(), timer.getRemaining())
                .handle((v, ex) -> {
                    if (ex != null && !(Exceptions.unwrap(ex) instanceof StreamSegmentSealedException)) {
                        // The operation failed, and it was not because the Segment was already Sealed. Throw it again.
                        // We consider the Seal to succeed if the Segment in Storage is already sealed - it's an idempotent operation.
                        throw new CompletionException(ex);
                    }

                    updateStatePostSeal();
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "sealIfNecessary", traceId, flushResult);
                    return flushResult;
                });
    }

    //endregion

    //region Reconciliation

    /**
     * Initiates the Storage reconciliation procedure. Gets the current state of the Segment from Storage, and based on that,
     * does one of the following:
     * * Nothing, if the Storage agrees with the Metadata.
     * * Throws a show-stopping DataCorruptionException (wrapped in a CompletionException) if the situation is unrecoverable.
     * * Initiates the Reconciliation Procedure, which allows the reconcile() method to execute.
     *
     * @param timer Timer for the operation.
     * @return A CompletableFuture that indicates when the operation completed.
     */
    private CompletableFuture<Void> beginReconciliation(TimeoutTimer timer) {
        assert this.state.get() == AggregatorState.ReconciliationNeeded : "beginReconciliation cannot be called if state == " + this.state;
        return this.storage
                .getStreamSegmentInfo(this.metadata.getName(), timer.getRemaining())
                .thenAccept(sp -> {
                    if (sp.getLength() > this.metadata.getLength()) {
                        // The length of the Segment in Storage is beyond what we have in our DurableLog. This is not
                        // possible in a correct scenario and is usually indicative of an internal bug or some other external
                        // actor altering the Segment. We cannot recover automatically from this situation.
                        throw new CompletionException(new ReconciliationFailureException("Actual Segment length in Storage is larger than the Metadata Length.", this.metadata, sp));
                    } else if (sp.getLength() < this.metadata.getStorageLength()) {
                        // The length of the Segment in Storage is less than what we thought it was. This is not possible
                        // in a correct scenario, and is usually indicative of an internal bug or a real data loss in Storage.
                        // We cannot recover automatically from this situation.
                        throw new CompletionException(new ReconciliationFailureException("Actual Segment length in Storage is smaller than the Metadata StorageLength.", this.metadata, sp));
                    } else if (sp.getLength() == this.metadata.getStorageLength()) {
                        // Nothing to do.
                        return;
                    }

                    // If we get here, it means we have work to do. Set the state accordingly and move on.
                    this.reconciliationState.set(new ReconciliationState(this.metadata, sp));
                    setState(AggregatorState.Reconciling);
                });
    }

    private CompletableFuture<FlushResult> reconcile(TimeoutTimer timer, Executor executor) {
        assert this.state.get() == AggregatorState.Reconciling : "reconcile cannot be called if state == " + this.state;
        ReconciliationState rc = this.reconciliationState.get();
        assert rc != null : "reconciliationState is null";
        SegmentProperties storageInfo = rc.getStorageInfo();
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "reconcile", rc);

        // Process each Operation in sequence, as long as its starting offset is less than ReconciliationState.getStorageInfo().getLength()
        FlushResult result = new FlushResult();
        AtomicBoolean exceededStorageLength = new AtomicBoolean(false);
        return Futures
                .loop(
                        () -> this.operations.size() > 0 && !exceededStorageLength.get(),
                        () -> {
                            StorageOperation op = this.operations.getFirst();
                            return reconcileOperation(op, storageInfo, timer, executor)
                                    .thenApply(partialFlushResult -> {
                                        if (op.getLastStreamSegmentOffset() >= storageInfo.getLength()) {
                                            // This operation crosses the boundary of StorageLength. It has been reconciled,
                                            // and as such it is the last operation that we need to inspect.
                                            exceededStorageLength.set(true);
                                        }

                                        log.info("{}: Reconciled {} ({}).", this.traceObjectId, op, partialFlushResult);
                                        return partialFlushResult;
                                    });
                        },
                        result::withFlushResult,
                        executor)
                .thenApply(v -> {
                    updateMetadata(storageInfo);
                    this.reconciliationState.set(null);
                    setState(AggregatorState.Writing);
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "reconcile", traceId, result);
                    return result;
                });
    }

    /**
     * Attempts to reconcile the given StorageOperation.
     *
     * @param op          The Operation to reconcile.
     * @param storageInfo The current state of the Segment in Storage.
     * @param timer       Timer for the operation.
     * @param executor    An Executor to use for async tasks.
     * @return A CompletableFuture containing a FlushResult with the number of bytes reconciled, or failed with a ReconciliationFailureException,
     * if the operation cannot be reconciled, based on the in-memory metadata or the current state of the Segment in Storage.
     */
    private CompletableFuture<FlushResult> reconcileOperation(StorageOperation op, SegmentProperties storageInfo, TimeoutTimer timer, Executor executor) {
        CompletableFuture<FlushResult> result;
        if (isAppendOperation(op)) {
            result = reconcileAppendOperation(op, storageInfo, timer, executor);
        } else if (op instanceof MergeTransactionOperation) {
            result = reconcileMergeOperation((MergeTransactionOperation) op, storageInfo, timer);
        } else if (op instanceof StreamSegmentSealOperation) {
            result = reconcileSealOperation(storageInfo);
        } else if (op instanceof StreamSegmentTruncateOperation) {
            // Nothing to reconcile here.
            updateStatePostTruncate();
            result = CompletableFuture.completedFuture(new FlushResult());
        } else {
            result = Futures.failedFuture(new ReconciliationFailureException(String.format("Operation '%s' is not supported for reconciliation.", op), this.metadata, storageInfo));
        }
        return result;
    }

    /**
     * Attempts to reconcile the given Append Operation. Since Append Operations can be partially flushed, reconciliation
     * may be for the full operation or for a part of it.
     *
     * @param op          The Operation (StreamSegmentAppendOperation or CachedStreamSegmentAppendOperation) to reconcile.
     * @param storageInfo The current state of the Segment in Storage.
     * @param timer       Timer for the operation.
     * @return A CompletableFuture containing a FlushResult with the number of bytes reconciled, or failed with a ReconciliationFailureException,
     * if the operation cannot be reconciled, based on the in-memory metadata or the current state of the Segment in Storage.
     */
    private CompletableFuture<FlushResult> reconcileAppendOperation(StorageOperation op, SegmentProperties storageInfo, TimeoutTimer timer, Executor executor) {
        Preconditions.checkArgument(op instanceof AggregatedAppendOperation, "Not given an append operation.");

        // Read data from Storage, and compare byte-by-byte.
        InputStream appendStream = this.dataSource.getAppendData(op.getStreamSegmentId(), op.getStreamSegmentOffset(), (int) op.getLength());
        if (appendStream == null) {
            return Futures.failedFuture(new ReconciliationFailureException(
                    String.format("Unable to reconcile operation '%s' because no append data is associated with it.", op), this.metadata, storageInfo));
        }

        // Only read as much data as we need.
        long readLength = Math.min(op.getLastStreamSegmentOffset(), storageInfo.getLength()) - op.getStreamSegmentOffset();
        assert readLength > 0 : "Append Operation to be reconciled is beyond the Segment's StorageLength " + op;
        AtomicInteger bytesReadSoFar = new AtomicInteger();

        // Read all data from storage.
        byte[] storageData = new byte[(int) readLength];
        return Futures
                .loop(
                        () -> bytesReadSoFar.get() < readLength,
                        () -> this.storage.read(this.handle.get(), op.getStreamSegmentOffset() + bytesReadSoFar.get(), storageData, bytesReadSoFar.get(), (int) readLength - bytesReadSoFar.get(), timer.getRemaining()),
                        bytesRead -> {
                            assert bytesRead > 0 : String.format("Unable to make any read progress when reconciling operation '%s' after reading %s bytes.", op, bytesReadSoFar);
                            bytesReadSoFar.addAndGet(bytesRead);
                        },
                        executor)
                .thenApply(v -> {
                    // Compare, byte-by-byte, the contents of the append.
                    verifySame(appendStream, storageData, op, storageInfo);
                    if (readLength >= op.getLength() && op.getLastStreamSegmentOffset() <= storageInfo.getLength()) {
                        // Operation has been completely validated; pop it off the list.
                        StorageOperation removedOp = this.operations.removeFirst();
                        assert op == removedOp : "Reconciled operation is not the same as removed operation";
                    }

                    return new FlushResult().withFlushedBytes(readLength);
                });
    }

    @SneakyThrows
    private void verifySame(InputStream appendStream, byte[] storageData, StorageOperation op, SegmentProperties storageInfo) {
        for (int i = 0; i < storageData.length; i++) {
            if ((byte) appendStream.read() != storageData[i]) {
                throw new ReconciliationFailureException(
                        String.format("Unable to reconcile operation '%s' because of data differences at SegmentOffset %d.", op, op.getStreamSegmentOffset() + i), this.metadata, storageInfo);
            }
        }
    }

    /**
     * Attempts to reconcile the given MergeTransactionOperation.
     *
     * @param op          The Operation to reconcile.
     * @param storageInfo The current state of the Segment in Storage.
     * @param timer       Timer for the operation
     * @return A CompletableFuture containing a FlushResult with the number of bytes reconciled, or failed with a ReconciliationFailureException,
     * if the operation cannot be reconciled, based on the in-memory metadata or the current state of the Segment in Storage.
     */
    private CompletableFuture<FlushResult> reconcileMergeOperation(MergeTransactionOperation op, SegmentProperties storageInfo, TimeoutTimer timer) {
        // Verify that the transaction segment is still registered in metadata.
        UpdateableSegmentMetadata transactionMeta = this.dataSource.getStreamSegmentMetadata(op.getTransactionSegmentId());
        if (transactionMeta == null || transactionMeta.isDeleted()) {
            return Futures.failedFuture(new ReconciliationFailureException(String.format("Cannot reconcile operation '%s' because the transaction segment is deleted or missing from the metadata.", op), this.metadata, storageInfo));
        }

        // Verify that the operation fits fully within this segment (mergers are atomic - they either merge all or nothing).
        if (op.getLastStreamSegmentOffset() > storageInfo.getLength()) {
            return Futures.failedFuture(new ReconciliationFailureException(String.format("Cannot reconcile operation '%s' because the transaction segment is not fully merged into the parent.", op), this.metadata, storageInfo));
        }

        // Verify that the transaction segment does not exist in Storage anymore.
        return this.storage
                .exists(transactionMeta.getName(), timer.getRemaining())
                .thenApply(exists -> {
                    if (exists) {
                        throw new CompletionException(new ReconciliationFailureException(
                                String.format("Cannot reconcile operation '%s' because the transaction segment still exists in Storage.", op), this.metadata, storageInfo));
                    }

                    // Pop the first operation off the list and update the metadata for the transaction segment.
                    StorageOperation processedOperation = this.operations.removeFirst();
                    assert processedOperation != null && processedOperation instanceof MergeTransactionOperation : "First outstanding operation was not a MergeTransactionOperation";

                    int newCount = this.mergeTransactionCount.decrementAndGet();
                    assert newCount >= 0 : "Negative value for mergeTransactionCount";

                    updateMetadataForTransactionPostMerger(transactionMeta);
                    return new FlushResult().withMergedBytes(op.getLength());
                });
    }

    /**
     * Attempts to reconcile a StreamSegmentSealOperation.
     *
     * @param storageInfo The current state of the Segment in Storage.
     * @return A CompletableFuture containing a FlushResult with the number of bytes reconciled, or failed with a ReconciliationFailureException,
     * if the operation cannot be reconciled, based on the in-memory metadata or the current state of the Segment in Storage.
     */
    private CompletableFuture<FlushResult> reconcileSealOperation(SegmentProperties storageInfo) {
        // All we need to do is verify that the Segment is actually sealed in Storage.
        if (storageInfo.isSealed()) {
            // Update metadata and the internal state (this also pops the first Op from the operation list).
            updateStatePostSeal();
            return CompletableFuture.completedFuture(new FlushResult()); // No bytes were flushed or merged.
        } else {
            // A Seal was encountered as an Operation that should have been processed (based on its offset),
            // but the Segment in Storage is not sealed.
            return Futures.failedFuture(new ReconciliationFailureException("Segment was supposed to be sealed in storage but it is not.", this.metadata, storageInfo));
        }
    }

    //endregion

    //region Helpers

    /**
     * Ensures the following conditions are met:
     * * Regular Operations: SegmentId matches this SegmentAggregator's SegmentId
     * * Transactions: TargetSegmentId/SegmentId matches this SegmentAggregator's SegmentId.
     *
     * @param operation The operation to check.
     * @throws IllegalArgumentException If any of the validations failed.
     */
    private void checkSegmentId(StorageOperation operation) {
        // All exceptions thrown from here are RuntimeExceptions (as opposed from DataCorruptionExceptions); they are indicative
        // of bad code (objects got routed to wrong SegmentAggregators) and not data corruption.
        if (operation instanceof MergeTransactionOperation) {
            Preconditions.checkArgument(
                    !this.metadata.isTransaction(),
                    "MergeTransactionOperations can only be added to the parent StreamSegment; received '%s'.", operation);

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
        boolean isTruncate = isTruncateOperation(operation);
        if (this.hasSealPending.get() && !isTruncate) {
            // Even though the DurableLog should take care of this, doesn't hurt to check again that we cannot add anything
            // after a StreamSegmentSealOperation.
            throw new DataCorruptionException(String.format("No operation except Truncation is allowed for a sealed segment; received '%s'.", operation));
        }

        // StreamSegmentAppendOperations need to be pre-processed into CachedStreamSegmentAppendOperations.
        Preconditions.checkArgument(!(operation instanceof StreamSegmentAppendOperation), "SegmentAggregator cannot process StreamSegmentAppendOperations.");

        // Verify operation offset against the lastAddedOffset (whether the last Op in the list or StorageLength).
        long offset = operation.getStreamSegmentOffset();
        long length = operation.getLength();
        Preconditions.checkArgument(offset >= 0, "Operation '%s' has an invalid offset (%s).", operation, operation.getStreamSegmentOffset());
        Preconditions.checkArgument(length >= 0, "Operation '%s' has an invalid length (%s).", operation, operation.getLength());

        // Check that operations are contiguous (only for the operations after the first one - as we initialize lastAddedOffset on the first op).
        if (isTruncate) {
            if (this.metadata.getStartOffset() < operation.getStreamSegmentOffset()) {
                throw new DataCorruptionException(String.format(
                        "StreamSegmentTruncateOperation '%s' has a truncation offset beyond the one in the Segment's Metadata. Expected: at most %d, actual: %d.",
                        operation,
                        this.metadata.getStartOffset(),
                        offset));
            }
        } else {
            long lastOffset = this.lastAddedOffset.get();
            if (lastOffset >= 0 && offset != lastOffset) {
                throw new DataCorruptionException(String.format("Wrong offset for Operation '%s'. Expected: %s, actual: %d.",
                        operation, this.lastAddedOffset, offset));
            }
        }

        // Check that the operation does not exceed the Length of the StreamSegment.
        if (offset + length > this.metadata.getLength()) {
            throw new DataCorruptionException(String.format(
                    "Operation '%s' has at least one byte beyond its Length. Offset = %d, Length = %d, Length = %d.",
                    operation,
                    offset,
                    length,
                    this.metadata.getLength()));
        }

        if (operation instanceof StreamSegmentSealOperation) {
            // For StreamSegmentSealOperations, we must ensure the offset of the operation is equal to the Length for the segment.
            if (this.metadata.getLength() != offset) {
                throw new DataCorruptionException(String.format(
                        "Wrong offset for Operation '%s'. Expected: %d (Length), actual: %d.",
                        operation,
                        this.metadata.getLength(),
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
        long newLength = this.metadata.getStorageLength() + flushArgs.getLength();
        this.metadata.setStorageLength(newLength);

        // Remove operations from the outstanding list as long as every single byte it contains has been committed.
        boolean reachedEnd = false;
        while (this.operations.size() > 0 && !reachedEnd) {
            StorageOperation first = this.operations.getFirst();
            long lastOffset = first.getLastStreamSegmentOffset();
            reachedEnd = lastOffset >= newLength;

            // Verify that if we did reach the 'newLength' offset, we were on an append operation. Anything else is indicative of a bug.
            assert reachedEnd || isAppendOperation(first) : "Flushed operation was not an Append.";
            if (lastOffset <= newLength) {
                this.operations.removeFirst();
            }
        }

        // Update the last flush checkpoint.
        this.lastFlush.set(this.timer.getElapsed());
        return new FlushResult().withFlushedBytes(flushArgs.getLength());
    }

    /**
     * Updates the metadata and the internal state after a Seal was completed.
     */
    private void updateStatePostSeal() {
        // Update metadata.
        this.metadata.markSealedInStorage();
        this.operations.removeFirst();

        // Validate we have no more unexpected items and then close (as we shouldn't be getting anything else).
        assert this.operations.size() - this.truncateCount.get() == 0 : "there are outstanding non-truncate operations after a Seal";
        this.hasSealPending.set(false);
        if (this.operations.size() == 0) {
            // No further operations are expected (The only valid operation after a Seal is Truncate, which is why we
            // can't assume there are no further operations.
            close();
        }
    }

    /**
     * Updates the internal state after a Truncate was completed.
     */
    private void updateStatePostTruncate() {
        this.operations.removeFirst();
        this.truncateCount.decrementAndGet();
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

    private void updateMetadataForTransactionPostMerger(UpdateableSegmentMetadata transactionMetadata) {
        // The other StreamSegment no longer exists and/or is no longer usable. Make sure it is marked as deleted.
        transactionMetadata.markDeleted();
        this.dataSource.deleteStreamSegment(transactionMetadata.getName()); // This may be redundant...

        // Complete the merger (in the ReadIndex and whatever other listeners we might have).
        this.dataSource.completeMerge(transactionMetadata.getParentId(), transactionMetadata.getId());
    }

    /**
     * Determines if the given StorageOperation is an Append Operation.
     *
     * @param op The operation to test.
     * @return True if an Append Operation (Cached or non-cached), false otherwise.
     */
    private boolean isAppendOperation(StorageOperation op) {
        return op != null && op instanceof AggregatedAppendOperation;
    }

    /**
     * Determines whether the given StorageOperation's Offset must be match the current expected Offset.
     */
    private boolean isTruncateOperation(StorageOperation operation) {
        return operation != null && operation instanceof StreamSegmentTruncateOperation;
    }

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(isClosed(), this);
        Preconditions.checkState(this.state.get() != AggregatorState.NotInitialized, "SegmentAggregator is not initialized. Cannot execute this operation.");
    }

    private void setState(AggregatorState newState) {
        AggregatorState oldState = this.state.get();
        if (newState != oldState) {
            log.info("{}: State changed from {} to {}.", this.traceObjectId, oldState, newState);
        }

        this.state.set(newState);
    }

    /**
     * Opens the given segment for writing.
     *
     * @param segmentName The segment to open.
     * @param handleRef   An AtomicReference that will contain the SegmentHandle for the opened segment.
     * @param executor    An executor to run async tasks.
     * @param timeout     Timeout for the operation.
     * @return A Future that will contain information about the opened Segment.
     */
    private CompletableFuture<SegmentProperties> openWrite(String segmentName, AtomicReference<SegmentHandle> handleRef, Executor executor, Duration timeout) {
        return this.storage
                .openWrite(segmentName)
                .thenComposeAsync(handle -> {
                    handleRef.set(handle);
                    return this.storage.getStreamSegmentInfo(segmentName, timeout);
                }, executor);
    }

    //endregion

    //region ReconciliationState

    @Getter
    private static class ReconciliationState {
        private final SegmentProperties storageInfo;
        private final long initialStorageLength;

        ReconciliationState(SegmentMetadata segmentMetadata, SegmentProperties storageInfo) {
            Preconditions.checkNotNull(storageInfo, "storageInfo");
            this.storageInfo = storageInfo;
            this.initialStorageLength = segmentMetadata.getStorageLength();
        }

        @Override
        public String toString() {
            return String.format("Metadata.StorageLength = %d, Storage.Length = %d", this.initialStorageLength, this.storageInfo.getLength());
        }
    }

    /**
     * Represents a set of arguments for a Storage Flush Operation.
     */
    @Getter
    @RequiredArgsConstructor
    private static class FlushArgs {
        private final InputStream stream;
        private final int length;

        @Override
        public String toString() {
            return String.format("TotalSize = %d", this.length);
        }
    }

    private static class AggregatedAppendOperation extends StorageOperation {
        private final AtomicLong streamSegmentOffset;
        private final AtomicInteger length;
        private final AtomicBoolean sealed;

        AggregatedAppendOperation(long streamSegmentId, long streamSegmentOffset, long sequenceNumber) {
            super(streamSegmentId);
            this.streamSegmentOffset = new AtomicLong(streamSegmentOffset);
            setSequenceNumber(sequenceNumber);
            this.length = new AtomicInteger();
            this.sealed = new AtomicBoolean();
        }

        void increaseLength(int amount) {
            Preconditions.checkArgument(amount > 0, "amount must be a positive integer.");
            this.length.addAndGet(amount);
        }

        void seal() {
            this.sealed.set(true);
        }

        boolean isSealed() {
            return this.sealed.get();
        }

        // region StorageOperation Implementation

        @Override
        public long getStreamSegmentOffset() {
            return this.streamSegmentOffset.get();
        }

        @Override
        public long getLength() {
            return this.length.get();
        }

        @Override
        protected OperationType getOperationType() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void serializeContent(DataOutputStream target) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void deserializeContent(DataInputStream source) throws IOException, SerializationException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return String.format("AggregatedAppend: SegmentId = %s, Offsets = [%s-%s), SeqNo = %s", getStreamSegmentId(), getStreamSegmentOffset(), getStreamSegmentOffset() + getLength(), getSequenceNumber());
        }

        //endregion
    }

    //endregion

    //region OperationQueue

    /**
     * Thin wrapper for a simple Queue[StorageOperation] that provides thread synchronization.
     */
    @ThreadSafe
    private static class OperationQueue {
        @GuardedBy("this")
        private final ArrayDeque<StorageOperation> queue = new ArrayDeque<>();

        synchronized boolean add(StorageOperation operation) {
            return this.queue.add(operation);
        }

        synchronized StorageOperation getLast() {
            return this.queue.peekLast();
        }

        synchronized StorageOperation getFirst() {
            return this.queue.peekFirst();
        }

        synchronized StorageOperation removeFirst() {
            return this.queue.pollFirst();
        }

        synchronized int size() {
            return this.queue.size();
        }
    }

    //endregion
}
