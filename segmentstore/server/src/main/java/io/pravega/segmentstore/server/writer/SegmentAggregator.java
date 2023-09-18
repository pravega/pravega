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
import io.pravega.common.AbstractTimer;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadOffsetException;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.ServiceHaltException;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.server.WriterFlushResult;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.DeleteSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.MergeSegmentOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.logs.operations.StorageOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentSealOperation;
import io.pravega.segmentstore.server.logs.operations.StreamSegmentTruncateOperation;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Cleanup;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/**
 * Aggregates contents for a specific StreamSegment.
 *
 * This class is responsible with applying the following Segment changes to Storage: Appends, Seals, Truncations, Mergers,
 * Creations and Deletions.
 *
 * For Segment Attributes, this class is only responsible with deleting the Segment Attribute Index when the main Segment
 * is deleted. For all other Attribute operations, refer to {@link AttributeAggregator}.
 */
@Slf4j
class SegmentAggregator implements WriterSegmentProcessor, AutoCloseable {
    //region Members

    private final UpdateableSegmentMetadata metadata;
    private final WriterConfig config;
    private final OperationQueue operations;
    private final AbstractTimer timer;
    private final Executor executor;
    private final String traceObjectId;
    private final Storage storage;
    private final AtomicReference<SegmentHandle> handle;
    private final WriterDataSource dataSource;
    private final AtomicInteger mergeTransactionCount;
    private final AtomicInteger truncateCount;
    private final AtomicBoolean hasSealPending;
    private final AtomicBoolean hasDeletePending;
    private final AtomicLong lastAddedOffset;
    private final AtomicReference<Duration> lastFlush;
    private final AtomicReference<AggregatorState> state;
    private final AtomicReference<ReconciliationState> reconciliationState;
    private final AggregatedAppendIntegrityChecker dataIntegrityChecker;

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
    SegmentAggregator(UpdateableSegmentMetadata segmentMetadata, WriterDataSource dataSource, Storage storage, WriterConfig config, AbstractTimer timer, Executor executor) {
        this.metadata = Preconditions.checkNotNull(segmentMetadata, "segmentMetadata");
        Preconditions.checkArgument(this.metadata.getContainerId() == dataSource.getId(), "SegmentMetadata.ContainerId is different from WriterDataSource.Id");
        this.traceObjectId = String.format("StorageWriter[%d-%d]", this.metadata.getContainerId(), this.metadata.getId());

        this.config = Preconditions.checkNotNull(config, "config");
        this.storage = Preconditions.checkNotNull(storage, "storage");
        this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
        this.timer = Preconditions.checkNotNull(timer, "timer");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.lastFlush = new AtomicReference<>(timer.getElapsed());
        this.lastAddedOffset = new AtomicLong(-1); // Will be set properly after we process a StorageOperation.
        this.mergeTransactionCount = new AtomicInteger();
        this.truncateCount = new AtomicInteger();
        this.hasSealPending = new AtomicBoolean();
        this.hasDeletePending = new AtomicBoolean();
        this.operations = new OperationQueue();
        this.state = new AtomicReference<>(AggregatorState.NotInitialized);
        this.reconciliationState = new AtomicReference<>();
        this.handle = new AtomicReference<>();
        this.dataIntegrityChecker = new AggregatedAppendIntegrityChecker(this.metadata.getContainerId(), this.metadata.getId());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!isClosed()) {
            setState(AggregatorState.Closed);
            this.dataIntegrityChecker.close();
        }
    }

    //endregion

    //region WriterSegmentProcessor Implementation

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
     * <li> The SegmentAggregator contains a StreamSegmentSealOperation or MergeSegmentOperation (hasSealPending == true)
     * <li> The SegmentAggregator is currently in a Reconciliation State (recovering from an inconsistency in Storage).
     * </ul>
     */
    @Override
    public boolean mustFlush() {
        if (this.metadata.isDeletedInStorage()) {
            // Already deleted in Storage. There isn't more that we can do.
            return false;
        }

        return exceedsThresholds()
                || this.hasDeletePending.get()
                || this.hasSealPending.get()
                || this.mergeTransactionCount.get() > 0
                || this.truncateCount.get() > 0
                || (this.operations.size() > 0 && isReconciling());
    }

    /**
     * Gets a value indicating whether the Flush thresholds are exceeded for this SegmentAggregator.
     */
    private boolean exceedsThresholds() {
        boolean isFirstAppend = this.operations.size() > 0 && isAppendOperation(this.operations.getFirst());
        long length = isFirstAppend ? this.operations.getFirst().getLength() : 0;
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
                "[%d: %s] Count = %d, LastOffset = %s, LUSN = %d, LastFlush = %ds",
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
     * @return A CompletableFuture that, when completed, will indicate that the operation finished successfully. If any
     * errors occurred during the operation, the Future will be completed with the appropriate exception.
     */
    CompletableFuture<Void> initialize(Duration timeout) {
        Exceptions.checkNotClosed(isClosed(), this);
        Preconditions.checkState(this.state.get() == AggregatorState.NotInitialized, "SegmentAggregator has already been initialized.");
        assert this.handle.get() == null : "non-null handle but state == " + this.state.get();
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "initialize");

        if (this.metadata.isDeleted()) {
            // Segment is dead on arrival. Delete it from Storage (if it exists) and do not bother to do anything else with it).
            // This is a rather uncommon case, but it can happen in one of two cases: 1) the segment has been deleted
            // immediately after creation or 2) after a container recovery.
            log.info("{}: Segment '{}' is marked as Deleted in Metadata. Attempting Storage delete.",
                    this.traceObjectId, this.metadata.getName());
            return Futures.exceptionallyExpecting(
                    this.storage.openWrite(this.metadata.getName())
                            .thenComposeAsync(handle -> this.storage.delete(handle, timeout), this.executor),
                    ex -> ex instanceof StreamSegmentNotExistsException, null) // It's OK if already deleted.
                    .thenRun(() -> {
                        updateMetadataPostDeletion(this.metadata);
                        log.info("{}: Segment '{}' is marked as Deleted in Metadata and has been deleted from Storage. Ignoring all further operations on it.",
                                this.traceObjectId, this.metadata.getName());
                        setState(AggregatorState.Writing);
                        LoggerHelpers.traceLeave(log, this.traceObjectId, "initialize", traceId);
                    });
        }

        // Segment not deleted.
        return openWrite(this.metadata.getName(), this.handle, timeout)
                .thenAcceptAsync(segmentInfo -> {
                    // Check & Update StorageLength in metadata.
                    if (this.metadata.getStorageLength() != segmentInfo.getLength()) {
                        if (this.metadata.getStorageLength() >= 0) {
                            // Only log warning if the StorageLength has actually been initialized, but is different.
                            log.info("{}: SegmentMetadata has a StorageLength ({}) that is different than the actual one ({}) - updating metadata.",
                                    this.traceObjectId, this.metadata.getStorageLength(), segmentInfo.getLength());
                        }

                        // It is very important to keep this value up-to-date and correct.
                        this.metadata.setStorageLength(segmentInfo.getLength());
                    }

                    // Check if the Storage segment is sealed, but it's not in metadata (this is 100% indicative of some data corruption happening).
                    if (segmentInfo.isSealed()) {
                        if (!this.metadata.isSealed()) {
                            throw new CompletionException(new DataCorruptionException(String.format(
                                    "Segment '%s' is sealed in Storage but not in the metadata.", this.metadata.getName())));
                        }

                        if (!this.metadata.isSealedInStorage()) {
                            this.metadata.markSealedInStorage();
                            log.info("{}: Segment is sealed in Storage but metadata does not reflect that - updating metadata.", this.traceObjectId);
                        }
                    }

                    log.info("{}: Initialized. StorageLength = {}, Sealed = {}.", this.traceObjectId, segmentInfo.getLength(), segmentInfo.isSealed());
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "initialize", traceId);
                    setState(AggregatorState.Writing);
                }, this.executor)
                .exceptionally(ex -> {
                    ex = Exceptions.unwrap(ex);
                    if (ex instanceof StreamSegmentNotExistsException) {
                        // Segment does not exist in Storage. There are two possibilities here:
                        if (this.metadata.getStorageLength() == 0 && !this.metadata.isDeletedInStorage()) {
                            // Segment has never been created because there was nothing to write to it. As long as we know
                            // its expected length is zero, this is a valid case.
                            this.handle.set(null);
                            log.info("{}: Initialized. Segment does not exist in Storage but Metadata indicates it should be empty.", this.traceObjectId);
                            if (this.metadata.isSealed() && this.metadata.getLength() == 0) {
                                // Truly an empty segment that is sealed; mark it as such in Storage.
                                this.metadata.markSealedInStorage();
                                log.info("{}: Segment does not exist in Storage, but Metadata indicates it is empty and sealed - marking as sealed in storage.", this.traceObjectId);
                            }
                        } else {
                            // Segment does not exist anymore. This is a real possibility during recovery, in the following cases:
                            // * We already processed a Segment Deletion but did not have a chance to checkpoint metadata
                            // * We processed a MergeSegmentOperation but did not have a chance to ack/truncate the DataSource
                            // Update metadata, just in case it is not already updated.
                            updateMetadataPostDeletion(this.metadata);
                            log.info("{}: Segment '{}' does not exist in Storage. Ignoring all further operations on it.",
                                    this.traceObjectId, this.metadata.getName());
                        }
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
     * Adds the given SegmentOperation to the Aggregator.
     *
     * @param operation the Operation to add.
     * @throws ServiceHaltException     If the validation of the given Operation indicates a possible data corruption in
     *                                  the code (offset gaps, out-of-order operations, etc.) or state in which the
     *                                  operation has been already acknowledged.
     * @throws IllegalArgumentException If the validation of the given Operation indicates a possible non-corrupting bug
     *                                  in the code.
     */
    @Override
    public void add(SegmentOperation operation) throws ServiceHaltException {
        ensureInitializedAndNotClosed();
        if (!(operation instanceof StorageOperation)) {
            // We only care about StorageOperations.
            return;
        }

        // Verify the operation is valid with respect to the state of this SegmentAggregator.
        StorageOperation storageOp = (StorageOperation) operation;
        checkValidOperation(storageOp);

        boolean isDelete = isDeleteOperation(storageOp);
        if (isDelete) {
            addDeleteOperation((DeleteSegmentOperation) storageOp);
            log.debug("{}: Add {}.", this.traceObjectId, storageOp);
        } else if (!this.metadata.isDeleted()) {
            // Process the operation.
            addStorageOperation(storageOp);
            log.debug("{}: Add {}; OpCount={}, MergeCount={}, Seal={}.", this.traceObjectId, storageOp,
                    this.operations.size(), this.mergeTransactionCount, this.hasSealPending);
        }
    }

    private void addDeleteOperation(DeleteSegmentOperation operation) {
        this.operations.add(operation);
        this.hasDeletePending.set(true);
    }

    private void addStorageOperation(StorageOperation operation) throws ServiceHaltException {
        checkValidStorageOperation(operation);

        // Add operation to list, but only if hasn't yet been persisted in Storage. It needs processing if either:
        // - It has at least one byte that hasn't yet been applied to Storage.
        // - It is a Merge Operation with an empty Source, but the Merge Offset is exactly at the Segment's last offset.
        // - It is a Truncate Operation.
        // - It is a Seal Operation but the Segment is not yet sealed in Storage.
        long lastOffset = operation.getLastStreamSegmentOffset();
        boolean isTruncate = isTruncateOperation(operation);
        boolean isMerge = operation instanceof MergeSegmentOperation;
        boolean processOp = lastOffset > this.metadata.getStorageLength()
                || isTruncate
                || (!this.metadata.isSealedInStorage() && (operation instanceof StreamSegmentSealOperation))
                || (isMerge && operation.getLength() == 0 && lastOffset == this.metadata.getStorageLength());
        if (processOp) {
            processNewOperation(operation);
        } else {
            acknowledgeAlreadyProcessedOperation(operation);
        }

        if (!isTruncate) {
            // Always record the last added offset, to ensure that operations are contiguous and processed in the right order.
            this.lastAddedOffset.set(lastOffset);
        }
    }

    /**
     * Processes an operation that would result in a change to the underlying Storage.
     *
     * @param operation The Operation to process.
     */
    private void processNewOperation(StorageOperation operation) {
        if (operation instanceof MergeSegmentOperation) {
            this.operations.add(operation);
            this.mergeTransactionCount.incrementAndGet();
        } else if (operation instanceof StreamSegmentSealOperation) {
            this.operations.add(operation);
            this.hasSealPending.set(true);
        } else if (operation instanceof StreamSegmentTruncateOperation) {
            this.operations.add(operation);
            this.truncateCount.incrementAndGet();
        } else if (operation instanceof CachedStreamSegmentAppendOperation) {
            // Track new append for integrity checks, if necessary,
            this.dataIntegrityChecker.addAppendIntegrityInfo(operation.getStreamSegmentId(), operation.getStreamSegmentOffset(),
                    operation.getLength(), ((CachedStreamSegmentAppendOperation) operation).getContentHash());
            // Aggregate the Append Operation.
            AggregatedAppendOperation aggregatedAppend = getOrCreateAggregatedAppend(
                    operation.getStreamSegmentOffset(), operation.getSequenceNumber());
            aggregateAppendOperation((CachedStreamSegmentAppendOperation) operation, aggregatedAppend);
        }
    }

    /**
     * Processes (acks) an operation that has already been processed and would otherwise result in no change to the
     * underlying Storage.
     *
     * @param operation The operation to handle.
     */
    private void acknowledgeAlreadyProcessedOperation(SegmentOperation operation) throws ServiceHaltException {
        if (operation instanceof MergeSegmentOperation) {
            // Only MergeSegmentOperations need special handling. Others, such as StreamSegmentSealOperation, are not
            // needed since they're handled in the initialize() method. Ensure that the DataSource is aware of this
            // (since after recovery, it may not know that a merge has been properly completed).
            MergeSegmentOperation mergeOp = (MergeSegmentOperation) operation;
            try {
                updateMetadataForTransactionPostMerger(this.dataSource.getStreamSegmentMetadata(mergeOp.getSourceSegmentId()), mergeOp.getStreamSegmentId());
            } catch (Throwable ex) {
                // Something really weird must have happened if we ended up in here. To prevent any (further) damage, we need
                // to stop the Segment Container right away.
                throw new ServiceHaltException(String.format("Unable to acknowledge already processed operation '%s'.", operation), ex);
            }
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
     */
    private void aggregateAppendOperation(CachedStreamSegmentAppendOperation operation, AggregatedAppendOperation aggregatedAppend) {
        long remainingLength = operation.getLength();
        if (operation.getStreamSegmentOffset() < aggregatedAppend.getLastStreamSegmentOffset()) {
            // The given operation begins before the AggregatedAppendOperation. This is likely due to it having been
            // partially written to Storage prior to some recovery event. We must make sure we only include the part that
            // has not yet been written.
            long delta = aggregatedAppend.getLastStreamSegmentOffset() - operation.getStreamSegmentOffset();
            remainingLength -= delta;
            log.debug("{}: Skipping {} bytes from the beginning of '{}' since it has already been partially written to Storage.", this.traceObjectId, delta, operation);
        }

        while (remainingLength > 0) {
            // All append lengths are integers, so it's safe to cast here.
            int lengthToAdd = (int) Math.min(this.config.getMaxFlushSizeBytes() - aggregatedAppend.getLength(), remainingLength);
            aggregatedAppend.increaseLength(lengthToAdd);
            remainingLength -= lengthToAdd;
            if (remainingLength > 0) {
                // We still have data to add, which means we filled up the current AggregatedAppend; make a new one.
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
     * @return The AggregatedAppend to use (existing or freshly created).
     */
    private AggregatedAppendOperation getOrCreateAggregatedAppend(long operationOffset, long operationSequenceNumber) {
        AggregatedAppendOperation aggregatedAppend = null;
        if (this.operations.size() > 0) {
            StorageOperation last = this.operations.getLast();
            if (last.getLength() < this.config.getMaxFlushSizeBytes() && isAppendOperation(last)) {
                aggregatedAppend = (AggregatedAppendOperation) last;
                if (aggregatedAppend.isSealed()) {
                    aggregatedAppend = null;
                }
            }
        }

        if (aggregatedAppend == null) {
            // No operations or last operation not an AggregatedAppend - create a new one, while making sure the first
            // offset is not below the current StorageLength (otherwise we risk re-writing data that's already in Storage).
            long offset = Math.max(operationOffset, this.metadata.getStorageLength());
            aggregatedAppend = new AggregatedAppendOperation(this.metadata.getId(), offset, operationSequenceNumber);
            this.operations.add(aggregatedAppend);
        }

        return aggregatedAppend;
    }

    //endregion

    //region Flushing and Merging

    /**
     * Flushes the contents of the Aggregator to the Storage.
     *
     * @param force   If true, force-flushes everything accumulated in the {@link SegmentAggregator}, regardless of
     *                the value returned by {@link #mustFlush()}.
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain a summary of the flush operation. If any errors
     * occurred during the flush, the Future will be completed with the appropriate exception.
     */
    @Override
    public CompletableFuture<WriterFlushResult> flush(boolean force, Duration timeout) {
        ensureInitializedAndNotClosed();
        if (this.metadata.isDeletedInStorage()) {
            // Segment has been deleted; don't do anything else.
            return CompletableFuture.completedFuture(new WriterFlushResult());
        }

        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "flush");

        TimeoutTimer timer = new TimeoutTimer(timeout);
        CompletableFuture<WriterFlushResult> result;
        try {
            switch (this.state.get()) {
                case Writing:
                    result = flushNormally(force, timer);
                    break;
                case ReconciliationNeeded:
                    result = beginReconciliation(timer)
                            .thenComposeAsync(v -> reconcile(timer), this.executor);
                    break;
                case Reconciling:
                    result = reconcile(timer);
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
     * @param force Whether to force everything out.
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation.
     */
    private CompletableFuture<WriterFlushResult> flushNormally(boolean force, TimeoutTimer timer) {
        assert this.state.get() == AggregatorState.Writing : "flushNormally cannot be called if state == " + this.state;
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "flushNormally", force, this.operations.size());
        WriterFlushResult result = new WriterFlushResult();
        AtomicBoolean canContinue = new AtomicBoolean(true);
        return Futures
                .loop(
                        canContinue::get,
                        () -> flushOnce(force, timer),
                        partialResult -> {
                            canContinue.set(partialResult.getFlushedBytes() + partialResult.getMergedBytes() > 0);
                            result.withFlushResult(partialResult);
                        },
                        this.executor)
                .thenApply(v -> {
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "flushNormally", traceId, result);
                    return result;
                });
    }

    /**
     * Flushes the contents of the Aggregator exactly once to the Storage in a 'normal' mode (where it does not need to
     * do any reconciliation).
     *
     * @param force Whether to force everything out.
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation.
     */
    private CompletableFuture<WriterFlushResult> flushOnce(boolean force, TimeoutTimer timer) {
        boolean hasDelete = this.hasDeletePending.get();
        boolean hasMerge = this.mergeTransactionCount.get() > 0;
        boolean hasSeal = this.hasSealPending.get();
        boolean hasTruncate = this.truncateCount.get() > 0;
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "flushOnce", this.operations.size(),
                this.mergeTransactionCount, hasSeal, hasTruncate, hasDelete);

        CompletableFuture<WriterFlushResult> result;
        if (hasDelete) {
            // If we have a Deletes, simply delete the Segment and move on. No other operation matters now.
            result = deleteSegment(timer);
        } else if (hasSeal || hasMerge || hasTruncate) {
            // If we have a Seal, Merge or Truncate Pending, flush everything until we reach that operation.
            result = flushFully(timer);
            if (hasMerge) {
                // If we have a merge, do it after we flush fully.
                result = result.thenComposeAsync(flushResult -> mergeIfNecessary(flushResult, timer), this.executor);
            }

            if (hasSeal) {
                // If we have a seal, do it after every other operation.
                result = result.thenComposeAsync(flushResult -> sealIfNecessary(flushResult, timer), this.executor);
            }
        } else {
            // Otherwise, either flush the excess as long as we have something to flush or flush everything out, depending
            // on whether we were asked to force the flush.
            result = force ? flushFully(timer) : flushExcess(timer);
        }

        if (log.isTraceEnabled()) {
            result = result.thenApply(r -> {
                LoggerHelpers.traceLeave(log, this.traceObjectId, "flushOnce", traceId, r);
                return r;
            });
        }

        return result;
    }

    /**
     * Flushes all Append Operations that can be flushed at the given moment (until the entire Aggregator is emptied out
     * or until a StreamSegmentSealOperation or MergeSegmentOperation is encountered).
     *
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation.
     */
    private CompletableFuture<WriterFlushResult> flushFully(TimeoutTimer timer) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "flushFully");
        WriterFlushResult result = new WriterFlushResult();
        return Futures
                .loop(
                        this::canContinueFlushingFully,
                        () -> flushPendingAppends(timer.getRemaining())
                                .thenCompose(flushResult -> flushPendingTruncate(flushResult, timer.getRemaining())),
                        result::withFlushResult,
                        this.executor)
                .thenApply(v -> {
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "flushFully", traceId, result);
                    return result;
                });
    }

    /**
     * Determines whether flushFully can continue given the current state of this SegmentAggregator.
     */
    private boolean canContinueFlushingFully() {
        if (this.metadata.isDeleted()) {
            return false; // No point in flushing if the segment has been deleted in the meantime.
        }

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
    private CompletableFuture<WriterFlushResult> flushExcess(TimeoutTimer timer) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "flushExcess");
        WriterFlushResult result = new WriterFlushResult();
        return Futures
                .loop(
                        this::canContinueFlushingExcess,
                        () -> flushPendingAppends(timer.getRemaining())
                                .thenCompose(flushResult -> flushPendingTruncate(flushResult, timer.getRemaining())),
                        result::withFlushResult,
                        this.executor)
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
    private CompletableFuture<WriterFlushResult> flushPendingTruncate(WriterFlushResult flushResult, Duration timeout) {
        StorageOperation op = this.operations.getFirst();
        if (!isTruncateOperation(op) || !this.storage.supportsTruncation()) {
            // Nothing to do.
            return CompletableFuture.completedFuture(flushResult);
        }

        CompletableFuture<Void> truncateTask;
        if (this.handle.get() == null) {
            // Segment has not been created yet.
            assert this.metadata.getStorageLength() == 0 : "handle is null but Metadata.getStorageLength is non-zero";
            truncateTask = CompletableFuture.completedFuture(null);
        } else {
            long truncateOffset = Math.min(this.metadata.getStorageLength(), op.getStreamSegmentOffset());
            truncateTask = this.storage.truncate(this.handle.get(), truncateOffset, timeout);
        }

        return truncateTask.thenApplyAsync(v -> {
            updateStatePostTruncate();
            return flushResult;
        }, this.executor);
    }

    /**
     * Flushes all Append Operations that can be flushed up to the maximum allowed flush size.
     *
     * @param timeout  Timeout for the operation.
     * @return A CompletableFuture that, when completed, will contain the result from the flush operation.
     */
    private CompletableFuture<WriterFlushResult> flushPendingAppends(Duration timeout) {
        // Gather an InputStream made up of all the operations we can flush.
        BufferView flushData;
        try {
            flushData = getFlushData();
        } catch (DataCorruptionException ex) {
            return Futures.failedFuture(ex);
        }

        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "flushPendingAppends");

        // Flush them.
        TimeoutTimer timer = new TimeoutTimer(timeout);
        CompletableFuture<Void> flush;
        if (flushData == null || flushData.getLength() == 0) {
            flush = CompletableFuture.completedFuture(null);
        } else {
            flush = createSegmentIfNecessary(
                    () -> this.storage.write(this.handle.get(), this.metadata.getStorageLength(), flushData.getReader(), flushData.getLength(), timer.getRemaining()),
                    timer.getRemaining());
        }

        return flush
                .thenApplyAsync(v -> {
                    WriterFlushResult result = updateStatePostFlush(flushData);
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "flushPendingAppends", traceId, result);
                    return result;
                }, this.executor)
                .exceptionally(ex -> {
                    if (Exceptions.unwrap(ex) instanceof BadOffsetException) {
                        // We attempted to write at an offset that already contained other data. This can happen for a number of
                        // reasons, but we do not have enough information here to determine why. We need to enter reconciliation
                        // mode, which will determine the actual state of the segment in storage and take appropriate actions.
                        setState(AggregatorState.ReconciliationNeeded);
                    }

                    // Rethrow all exceptions.
                    throw new CompletionException(ex);
                });
    }

    /**
     * Returns a {@link BufferView} which contains the data needing to be flushed to Storage.
     *
     * @return A {@link BufferView} to flush or null if the segment was deleted.
     * @throws DataCorruptionException If a unable to retrieve required data from the Data Source.
     */
    @Nullable
    private BufferView getFlushData() throws DataCorruptionException {
        StorageOperation first = this.operations.getFirst();
        if (!(first instanceof AggregatedAppendOperation)) {
            // Nothing to flush - first operation is not an AggregatedAppend.
            return null;
        }

        AggregatedAppendOperation appendOp = (AggregatedAppendOperation) first;
        int length = (int) appendOp.getLength();
        BufferView data = null;
        if (length > 0) {
            data = this.dataSource.getAppendData(appendOp.getStreamSegmentId(), appendOp.getStreamSegmentOffset(), length);
            if (data == null) {
                if (this.metadata.isDeleted()) {
                    // Segment was deleted - nothing more to do.
                    return null;
                }
                throw new DataCorruptionException(String.format("Unable to retrieve CacheContents for '%s'.", appendOp));
            }

            // If configured, verify that the data received here is the same that was initially sent by the client.
            this.dataIntegrityChecker.checkAppendIntegrity(appendOp.getStreamSegmentId(), appendOp.getStreamSegmentOffset(), data);
        }

        appendOp.seal();
        return data;
    }

    /**
     * Executes a merger of a Transaction StreamSegment into this one.
     * Conditions for merger:
     * <ul>
     * <li> This StreamSegment is stand-alone (not a Transaction).
     * <li> The next outstanding operation is a MergeSegmentOperation for a Transaction StreamSegment of this StreamSegment.
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
    private CompletableFuture<WriterFlushResult> mergeIfNecessary(WriterFlushResult flushResult, TimeoutTimer timer) {
        ensureInitializedAndNotClosed();
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "mergeIfNecessary");

        StorageOperation first = this.operations.getFirst();
        if (first == null || !(first instanceof MergeSegmentOperation)) {
            // Either no operation or first operation is not a MergeTransaction. Nothing to do.
            LoggerHelpers.traceLeave(log, this.traceObjectId, "mergeIfNecessary", traceId, flushResult);
            return CompletableFuture.completedFuture(flushResult);
        }

        MergeSegmentOperation mergeSegmentOperation = (MergeSegmentOperation) first;
        UpdateableSegmentMetadata transactionMetadata = this.dataSource.getStreamSegmentMetadata(mergeSegmentOperation.getSourceSegmentId());
        return mergeWith(transactionMetadata, mergeSegmentOperation, timer)
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
    private CompletableFuture<WriterFlushResult> mergeWith(UpdateableSegmentMetadata transactionMetadata, MergeSegmentOperation mergeOp, TimeoutTimer timer) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "mergeWith", transactionMetadata.getId(), transactionMetadata.getName(), transactionMetadata.isSealedInStorage());

        boolean emptySourceSegment = transactionMetadata.getLength() == 0;
        if (transactionMetadata.isDeleted() && !emptySourceSegment) {
            // We came across a deleted source segment that had some data. We need to begin a reconciliation to figure out
            // the actual state of the segments in Storage.
            setState(AggregatorState.ReconciliationNeeded);
            return Futures.failedFuture(new StreamSegmentNotExistsException(transactionMetadata.getName()));
        }

        WriterFlushResult result = new WriterFlushResult();

        CompletableFuture<SegmentProperties> merge;
        if (emptySourceSegment) {
            // We came across a deleted source segment which had no data. No point in attempting to do anything, as any
            // operation involving this segment will complain about it not being there.
            log.warn("{}: Not applying '{}' because source segment is missing or empty.", this.traceObjectId, mergeOp);
            merge = CompletableFuture.completedFuture(this.metadata);
        } else if (!transactionMetadata.isSealedInStorage() || transactionMetadata.getLength() > transactionMetadata.getStorageLength()) {
            // Nothing to do. Given Transaction is not eligible for merger yet.
            LoggerHelpers.traceLeave(log, this.traceObjectId, "mergeWith", traceId, result);
            return CompletableFuture.completedFuture(result);
        } else {
            merge = mergeInStorage(transactionMetadata, mergeOp, timer);
        }

        // For simplicity, we will be deleting the Attribute Index here (and not in AttributeAggregator); this way we don't
        // need to make AttributeAggregator aware of merging segments.
        return merge
                .thenAcceptAsync(segmentProperties -> mergeCompleted(segmentProperties, transactionMetadata, mergeOp), this.executor)
                .thenComposeAsync(v -> this.dataSource.deleteAllAttributes(transactionMetadata, timer.getRemaining()), this.executor)
                .thenApply(v -> {
                    this.lastFlush.set(this.timer.getElapsed());
                    result.withMergedBytes(mergeOp.getLength());
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "mergeWith", traceId, result);
                    return result;
                })
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
     * Executes the merge of the Source StreamSegment with given metadata into this one in Storage.
     */
    private CompletableFuture<SegmentProperties> mergeInStorage(SegmentMetadata transactionMetadata, MergeSegmentOperation mergeOp, TimeoutTimer timer) {
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
                                "Transaction Segment '%s' cannot be merged into parent '%s' because the declared length in the operation disagrees with the Storage. "
                              + "Operation.Length=%d, Storage.StorageLength=%d",
                                transactionMetadata.getName(),
                                this.metadata.getName(),
                                mergeOp.getLength(),
                                transProperties.getLength())));
                    }
                }, this.executor)
                .thenComposeAsync(v -> createSegmentIfNecessary(
                        () -> storage.concat(this.handle.get(), mergeOp.getStreamSegmentOffset(), transactionMetadata.getName(), timer.getRemaining()),
                        timer.getRemaining()), this.executor)
                .exceptionally(ex -> {
                    ex = Exceptions.unwrap(ex);
                    if (transactionMetadata.getLength() == 0
                            && ex instanceof StreamSegmentNotExistsException
                            && ((StreamSegmentNotExistsException) ex).getStreamSegmentName().equals(transactionMetadata.getName())) {
                        log.warn("{}: Not applying '{}' because source segment is missing (storage) and had no data.", this.traceObjectId, mergeOp);
                        return null;
                    } else {
                        throw new CompletionException(ex);
                    }
                })
                .thenComposeAsync(v -> storage.getStreamSegmentInfo(this.metadata.getName(), timer.getRemaining()), this.executor);
    }

    /**
     * Executes post-Storage merge tasks, including state and metadata updates.
     */
    private void mergeCompleted(SegmentProperties segmentProperties, UpdateableSegmentMetadata transactionMetadata, MergeSegmentOperation mergeOp) {
        // We have processed a MergeSegmentOperation, pop the first operation off and decrement the counter.
        StorageOperation processedOperation = this.operations.removeFirst();
        assert processedOperation != null && processedOperation instanceof MergeSegmentOperation : "First outstanding operation was not a MergeSegmentOperation";
        MergeSegmentOperation mop = (MergeSegmentOperation) processedOperation;
        assert mop.getSourceSegmentId() == transactionMetadata.getId() : "First outstanding operation was a MergeSegmentOperation for the wrong Transaction id.";
        int newCount = this.mergeTransactionCount.decrementAndGet();
        assert newCount >= 0 : "Negative value for mergeTransactionCount";

        // Post-merger validation. Verify we are still in agreement with the storage.
        long expectedNewLength = this.metadata.getStorageLength() + mergeOp.getLength();
        if (segmentProperties.getLength() != expectedNewLength) {
            throw new CompletionException(new DataCorruptionException(String.format(
                    "Transaction Segment '%s' was merged into parent '%s' but the parent segment has an unexpected StorageLength after the merger. Previous=%d, MergeLength=%d, Expected=%d, Actual=%d",
                    transactionMetadata.getName(),
                    this.metadata.getName(),
                    segmentProperties.getLength(),
                    mergeOp.getLength(),
                    expectedNewLength,
                    segmentProperties.getLength())));
        }

        updateMetadata(segmentProperties);
        updateMetadataForTransactionPostMerger(transactionMetadata, mop.getStreamSegmentId());
    }

    /**
     * Seals the StreamSegment in Storage, if necessary.
     *
     * @param flushResult The FlushResult from a previous Flush operation. This will just be passed-through.
     * @param timer       Timer for the operation.
     * @return The FlushResult passed in as an argument.
     */
    private CompletableFuture<WriterFlushResult> sealIfNecessary(WriterFlushResult flushResult, TimeoutTimer timer) {
        if (!this.hasSealPending.get() || !(this.operations.getFirst() instanceof StreamSegmentSealOperation)) {
            // Either no Seal is pending or the next operation is not a seal - we cannot execute a seal.
            return CompletableFuture.completedFuture(flushResult);
        }

        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "sealIfNecessary");
        CompletableFuture<Void> sealTask;
        if (this.handle.get() == null) {
            // Segment is empty. It might not have been created yet, so don't do anything.
            assert this.metadata.getStorageLength() == 0 : "handle is null but Metadata.StorageLength is non-zero";
            sealTask = CompletableFuture.completedFuture(null);
        } else {
            // Segment is non-empty; it should exist.
            sealTask = this.storage.seal(this.handle.get(), timer.getRemaining());
        }

        return sealTask
                .handleAsync((v, ex) -> {
                    ex = Exceptions.unwrap(ex);
                    if (ex != null && !(ex instanceof StreamSegmentSealedException)) {
                        // The operation failed, and it was not because the Segment was already Sealed. Throw it again.
                        // We consider the Seal to succeed if the Segment in Storage is already sealed - it's an idempotent operation.
                        if (ex instanceof StreamSegmentNotExistsException) {
                            // The segment does not exist (anymore). This is possible if it got merged into another one.
                            // Begin a reconciliation to determine the actual state.
                            setState(AggregatorState.ReconciliationNeeded);
                        }
                        throw new CompletionException(ex);
                    }

                    updateStatePostSeal();
                    LoggerHelpers.traceLeave(log, this.traceObjectId, "sealIfNecessary", traceId, flushResult);
                    return flushResult;
                }, this.executor);
    }

    /**
     * Deletes the Segment handled by this {@link SegmentAggregator} instance and its Attributes, and updates the internal
     * Metadata and any other state to reflect the fact. This will also delete any other Segments that have pending Mergers
     * into this Segment, including any associated Attributes. Upon a successful completion, the internal Metadata and
     * {@link SegmentAggregator} state will be updated to reflect the Storage deletion.
     *
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate that the Segment and any other Storage
     * structures associated with it have been deleted.
     */
    private CompletableFuture<WriterFlushResult> deleteSegment(TimeoutTimer timer) {
        // Delete the Segment from Storage, but also delete any source Segments that had pending mergers. If we do not,
        // we will be left with orphaned Segments in Storage.
        CompletableFuture<Void> deleteFuture;
        if (this.handle.get() == null) {
            // Segment does not exist in Storage (most likely due to no data appended to it). However the Attribute Index
            // may exist since we may have persisted attribute updates. Make sure it is cleaned up in that case.
            deleteFuture = this.dataSource.deleteAllAttributes(metadata, timer.getRemaining());
        } else {
            deleteFuture = deleteSegmentAndAttributes(handle.get(), this.metadata, timer);
        }

        return deleteFuture
                .thenComposeAsync(v -> deleteUnmergedSourceSegments(timer), this.executor)
                .thenApplyAsync(v -> {
                    updateMetadataPostDeletion(this.metadata);
                    this.hasSealPending.set(false);
                    this.hasDeletePending.set(false);
                    this.truncateCount.set(0);
                    this.mergeTransactionCount.set(0);
                    this.operations.clear(); // No point in executing any other operation now.
                    return new WriterFlushResult();
                }, this.executor);
    }

    /**
     * Deletes a Segment and its Attributes from Storage. The Segment to delete need not be the same Segment handled by
     * this SegmentAggregator instance.
     *
     * This method does not update any metadata or any other in-memory state.
     *
     * @param handle   A {@link SegmentHandle} representing the Segment to delete.
     *                 this SegmentAggregator instance.
     * @param metadata The {@link SegmentMetadata} for the Segment to delete.
     * @param timer    Timer for the operation.
     * @return A CompletableFuture that, when completed, will indicate the given Segment and its Attributes have been
     * deleted from Storage. This method will not fail if either the Segment or its Attributes do not exist in Storage.
     */
    private CompletableFuture<Void> deleteSegmentAndAttributes(SegmentHandle handle, SegmentMetadata metadata, TimeoutTimer timer) {
        assert handle.getSegmentName().equals(metadata.getName());
        return CompletableFuture.allOf(
                Futures.exceptionallyExpecting(
                        this.storage.delete(handle, timer.getRemaining()),
                        ex -> ex instanceof StreamSegmentNotExistsException,
                        null),
                this.dataSource.deleteAllAttributes(metadata, timer.getRemaining()));
    }

    /**
     * Deletes all Segments from Storage that have a pending Merge into the Segment handled by this {@link SegmentAggregator}
     * instance. This will also delete other Storage structures related to those segments (such as Attributes) and will
     * update the internal Metadata to reflect the deletion.
     *
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed normally, will indicate all Segments with pending mergers have
     * been deleted from Storage.
     */
    private CompletableFuture<Void> deleteUnmergedSourceSegments(TimeoutTimer timer) {
        if (this.mergeTransactionCount.get() == 0) {
            return CompletableFuture.completedFuture(null);
        }

        // Identify all MergeSegmentOperations, pick up their names and delete them. There is no need to filter out these
        // operations or otherwise update the state of the Aggregator as this is part of a Delete operation, so resetting
        // the state is the next thing that will happen.
        List<CompletableFuture<Void>> toDelete = this.operations.filter(op -> op instanceof MergeSegmentOperation).stream()
                .map(op -> {
                    // Found such a merge; get the source Segment's name and attempt to delete it. It's OK if it has already
                    // been deleted.
                    UpdateableSegmentMetadata m = this.dataSource.getStreamSegmentMetadata(((MergeSegmentOperation) op).getSourceSegmentId());
                    return Futures
                            .exceptionallyExpecting(
                                    this.storage.openWrite(m.getName())
                                            .thenCompose(handle -> deleteSegmentAndAttributes(handle, m, timer))
                                            .thenAcceptAsync(v -> updateMetadataPostDeletion(m), this.executor),
                                    ex -> ex instanceof StreamSegmentNotExistsException,
                                    null);
                })
                .collect(Collectors.toList());
        return Futures.allOf(toDelete);
    }

    private CompletableFuture<Void> createSegmentIfNecessary(Supplier<CompletableFuture<Void>> toRun, Duration timeout) {
        if (this.handle.get() == null) {
            // No handle so, the segment must not exist yet. Attempt to create it, then run what we wanted to.
            assert this.metadata.getStorageLength() == 0 : "no handle yet but metadata indicates Storage Segment not empty";
            return Futures
                    .exceptionallyComposeExpecting(
                            this.storage.create(this.metadata.getName(), new SegmentRollingPolicy(getRolloverSize()), timeout),
                            ex -> ex instanceof StreamSegmentExistsException,
                            () -> {
                                // This happens if we have more than one concurrent instances of the owning SegmentContainer
                                // running at the same time. Both SegmentAggregator instances were initialized when the Segment
                                // did not exist, and both knew about an append that would eventually make it to Storage. One
                                // of them managed to create the Segment (and write something to it), but the other still assumed
                                // the Segment did not exist - so we end up in here. We need to get a handle of the segment
                                // and continue with whatever we were doing. If there is a mismatch (length, sealed, etc.),
                                // then the normal reconciliation algorithm will kick in once it is discovered and if the
                                // segment has already been fenced out, openWrite() will throw the appropriate exception
                                // which will be handled upstream.
                                log.info("{}: Segment did not exist in Storage when initialize() was called, but does now.", this.traceObjectId);
                                return this.storage.openWrite(this.metadata.getName());
                            })
                    .thenComposeAsync(handle -> {
                        this.handle.set(handle);
                        return toRun.get();
                    }, this.executor);
        } else {
            // Segment already exists. Execute what we were supposed to.
            return toRun.get();
        }
    }

    private long getRolloverSize() {
        // Configured value.
        long rolloverSize = this.metadata.getAttributes().getOrDefault(Attributes.ROLLOVER_SIZE, SegmentRollingPolicy.NO_ROLLING.getMaxLength());

        // rolloverSize being zero means the default value should be used.
        rolloverSize = rolloverSize == 0 ? SegmentRollingPolicy.NO_ROLLING.getMaxLength() : rolloverSize;

        // Make sure it does not exceed configured max value.
        return Math.min(rolloverSize, this.config.getMaxRolloverSize());
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
     * @param timer    Timer for the operation.
     * @return A CompletableFuture that indicates when the operation completed.
     */
    private CompletableFuture<Void> beginReconciliation(TimeoutTimer timer) {
        assert this.state.get() == AggregatorState.ReconciliationNeeded : "beginReconciliation cannot be called if state == " + this.state;
        return this.storage
                .getStreamSegmentInfo(this.metadata.getName(), timer.getRemaining())
                .thenAcceptAsync(sp -> {
                    if (sp.getLength() > this.metadata.getLength()) {
                        // The length of the Segment in Storage is beyond what we have in our DurableLog. This is not
                        // possible in a correct scenario and is usually indicative of an internal bug or some other external
                        // actor altering the Segment. We cannot recover automatically from this situation.
                        throw new CompletionException(new ReconciliationFailureException(
                                "Actual Segment length in Storage is larger than the Metadata Length.", this.metadata, sp));
                    } else if (sp.getLength() < this.metadata.getStorageLength()) {
                        // The length of the Segment in Storage is less than what we thought it was. This is not possible
                        // in a correct scenario, and is usually indicative of an internal bug or a real data loss in Storage.
                        // We cannot recover automatically from this situation.
                        throw new CompletionException(new ReconciliationFailureException(
                                "Actual Segment length in Storage is smaller than the Metadata StorageLength.", this.metadata, sp));
                    } else if (sp.getLength() == this.metadata.getStorageLength() && sp.isSealed() == this.metadata.isSealedInStorage()) {
                        // Nothing to do. Exit reconciliation and re-enter normal writing mode.
                        setState(AggregatorState.Writing);
                        return;
                    }

                    // If we get here, it means we have work to do. Set the state accordingly and move on.
                    this.reconciliationState.set(new ReconciliationState(this.metadata, sp));
                    setState(AggregatorState.Reconciling);
                }, this.executor)
                .exceptionally(ex -> {
                    ex = Exceptions.unwrap(ex);
                    if (ex instanceof StreamSegmentNotExistsException) {
                        // Segment does not exist in Storage. There are three possible situations:
                        if (this.metadata.isMerged() || this.metadata.isDeleted()) {
                            // Segment has actually been deleted. This is either due to un-acknowledged Merge/Delete operations
                            // or because of a concurrent instance of the same container (with a lower epoch) is still
                            // running and was in the middle of executing the Merge/Delete operation while we were initializing.
                            updateMetadataPostDeletion(this.metadata);
                            log.info("{}: Segment '{}' does not exist in Storage (reconciliation). Ignoring all further operations on it.",
                                    this.traceObjectId, this.metadata.getName());
                            this.reconciliationState.set(null);
                            setState(AggregatorState.Reconciling);
                        } else if (this.metadata.getStorageLength() > 0) {
                            // Segment should have existed in Storage, with data, but it does not. This is not possible
                            // in a correct scenario, and is usually indicative of an internal bug or a real data loss in
                            // Storage. We cannot recover automatically from this situation.
                            throw new CompletionException(new ReconciliationFailureException("Segment does not exist in Storage, but Metadata StorageLength is non-zero.",
                                    this.metadata, StreamSegmentInformation.builder().name(this.metadata.getName()).deleted(true).build()));
                        } else {
                            // Segment does not exist in Storage, but the Metadata indicates it should be empty. This is
                            // a valid situation since we may not have had a chance to create it yet.
                            this.reconciliationState.set(new ReconciliationState(this.metadata,
                                    StreamSegmentInformation.builder().name(this.metadata.getName()).build()));
                            setState(AggregatorState.Reconciling);
                        }
                    } else {
                        // Other kind of error - re-throw.
                        throw new CompletionException(ex);
                    }

                    return null;
                });
    }

    private CompletableFuture<WriterFlushResult> reconcile(TimeoutTimer timer) {
        ReconciliationState rc = this.reconciliationState.get();
        WriterFlushResult result = new WriterFlushResult();
        if (rc == null) {
            setState(AggregatorState.Writing);
            return CompletableFuture.completedFuture(result);
        } else if (this.hasDeletePending.get()) {
            // Do not bother with anything else. If we know we are going to delete this segment, then do no bother doing
            // any other kind of reconciliation work.
            setState(AggregatorState.Writing);
            return deleteSegment(timer);
        }

        SegmentProperties storageInfo = rc.getStorageInfo();
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "reconcile", rc);

        // Process each Operation in sequence, as long as its starting offset is less than ReconciliationState.getStorageInfo().getLength()
        AtomicBoolean exceededStorageLength = new AtomicBoolean(false);
        return Futures
                .loop(
                        () -> this.operations.size() > 0 && !exceededStorageLength.get(),
                        () -> {
                            StorageOperation op = this.operations.getFirst();
                            return reconcileOperation(op, storageInfo, timer)
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
                        this.executor)
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
     * @return A CompletableFuture containing a FlushResult with the number of bytes reconciled, or failed with a ReconciliationFailureException,
     * if the operation cannot be reconciled, based on the in-memory metadata or the current state of the Segment in Storage.
     */
    private CompletableFuture<WriterFlushResult> reconcileOperation(StorageOperation op, SegmentProperties storageInfo, TimeoutTimer timer) {
        CompletableFuture<WriterFlushResult> result;

        if (isAppendOperation(op)) {
            result = reconcileAppendOperation((AggregatedAppendOperation) op, storageInfo, timer);
        } else if (op instanceof MergeSegmentOperation) {
            result = reconcileMergeOperation((MergeSegmentOperation) op, storageInfo, timer);
        } else if (op instanceof StreamSegmentSealOperation) {
            result = reconcileSealOperation(storageInfo, timer.getRemaining());
        } else if (isTruncateOperation(op)) {
            // Nothing to reconcile here.
            updateStatePostTruncate();
            result = CompletableFuture.completedFuture(new WriterFlushResult());
        } else {
            result = Futures.failedFuture(new ReconciliationFailureException(String.format("Operation '%s' is not supported for reconciliation.", op), this.metadata, storageInfo));
        }
        return result;
    }

    /**
     * Attempts to reconcile data for the given AggregatedAppendOperation. Since Append Operations can be partially
     * flushed, reconciliation may be for the full operation or for a part of it.
     *
     * @param op          The AggregatedAppendOperation to reconcile.
     * @param storageInfo The current state of the Segment in Storage.
     * @param timer       Timer for the operation.
     * @return A CompletableFuture containing a FlushResult with the number of bytes reconciled, or failed with a ReconciliationFailureException,
     * if the operation cannot be reconciled, based on the in-memory metadata or the current state of the Segment in Storage.
     */
    private CompletableFuture<WriterFlushResult> reconcileAppendOperation(AggregatedAppendOperation op, SegmentProperties storageInfo, TimeoutTimer timer) {
        CompletableFuture<Integer> reconcileResult = op.getLength() > 0 ? reconcileData(op, storageInfo, timer) : CompletableFuture.completedFuture(0);
        return reconcileResult.thenApplyAsync(reconciledBytes -> {
            op.reconcileComplete(reconciledBytes); // Reflect the reconciliation result in the operation.
            if (op.getLength() == 0) {
                // Operation has been completely validated; pop it off the list.
                StorageOperation removedOp = this.operations.removeFirst();
                assert op == removedOp : "Reconciled operation is not the same as removed operation";
            }

            return new WriterFlushResult().withFlushedBytes(reconciledBytes);
        }, this.executor);
    }

    /**
     * Attempts to reconcile the data for the given AggregatedAppendOperation. Since Append Operations can be partially
     * flushed, reconciliation may be for the full operation or for a part of it.
     *
     * @param op          The AggregatedAppendOperation to reconcile.
     * @param storageInfo The current state of the Segment in Storage.
     * @param timer       Timer for the operation.
     * @return A CompletableFuture containing a FlushResult with the number of bytes reconciled, or failed with a ReconciliationFailureException,
     * if the operation cannot be reconciled, based on the in-memory metadata or the current state of the Segment in Storage.
     */
    private CompletableFuture<Integer> reconcileData(AggregatedAppendOperation op, SegmentProperties storageInfo, TimeoutTimer timer) {
        BufferView appendData = this.dataSource.getAppendData(op.getStreamSegmentId(), op.getStreamSegmentOffset(), (int) op.getLength());
        if (appendData == null) {
            return Futures.failedFuture(new ReconciliationFailureException(
                    String.format("Unable to reconcile operation '%s' because no append data is associated with it.", op), this.metadata, storageInfo));
        }

        // Only read as much data as we need.
        long readLength = Math.min(op.getLastStreamSegmentOffset(), storageInfo.getLength()) - op.getStreamSegmentOffset();
        assert readLength > 0 : "Append Operation to be reconciled is beyond the Segment's StorageLength (" + storageInfo.getLength() + "): " + op;

        // Read all data from storage.
        byte[] storageData = new byte[(int) readLength];
        AtomicInteger reconciledBytes = new AtomicInteger();
        return Futures
                .loop(
                        () -> reconciledBytes.get() < readLength,
                        () -> this.storage.read(this.handle.get(), op.getStreamSegmentOffset() + reconciledBytes.get(), storageData,
                                                reconciledBytes.get(), (int) readLength - reconciledBytes.get(), timer.getRemaining()),
                        bytesRead -> {
                            assert bytesRead > 0 : String.format("Unable to make any read progress when reconciling operation '%s' after reading %s bytes.", op, reconciledBytes);
                            reconciledBytes.addAndGet(bytesRead);
                        },
                        this.executor)
                .thenApplyAsync(v -> {
                    // Compare, byte-by-byte, the contents of the append.
                    verifySame(appendData, storageData, op, storageInfo);
                    return reconciledBytes.get();
                }, this.executor);
    }

    @SneakyThrows
    private void verifySame(BufferView appendData, byte[] storageData, StorageOperation op, SegmentProperties storageInfo) {
        @Cleanup
        InputStream appendStream = appendData.getReader();
        for (int i = 0; i < storageData.length; i++) {
            if ((byte) appendStream.read() != storageData[i]) {
                throw new ReconciliationFailureException(
                        String.format("Unable to reconcile operation '%s' because of data differences at SegmentOffset %d.", op, op.getStreamSegmentOffset() + i), this.metadata, storageInfo);
            }
        }
    }

    /**
     * Attempts to reconcile the given MergeSegmentOperation.
     *
     * @param op          The Operation to reconcile.
     * @param storageInfo The current state of the Segment in Storage.
     * @param timer       Timer for the operation
     * @return A CompletableFuture containing a FlushResult with the number of bytes reconciled, or failed with a ReconciliationFailureException,
     * if the operation cannot be reconciled, based on the in-memory metadata or the current state of the Segment in Storage.
     */
    private CompletableFuture<WriterFlushResult> reconcileMergeOperation(MergeSegmentOperation op, SegmentProperties storageInfo, TimeoutTimer timer) {
        // Verify that the transaction segment is still registered in metadata.
        UpdateableSegmentMetadata transactionMeta = this.dataSource.getStreamSegmentMetadata(op.getSourceSegmentId());
        if (transactionMeta == null) {
            return Futures.failedFuture(new ReconciliationFailureException(String.format(
                    "Cannot reconcile operation '%s' because the source segment is missing from the metadata.", op),
                    this.metadata, storageInfo));
        }

        // Verify that the operation fits fully within this segment (mergers are atomic - they either merge all or nothing).
        if (op.getLastStreamSegmentOffset() > storageInfo.getLength()) {
            return Futures.failedFuture(new ReconciliationFailureException(String.format(
                    "Cannot reconcile operation '%s' because the source segment is not fully merged into the target.", op),
                    this.metadata, storageInfo));
        }

        // Verify that the transaction segment does not exist in Storage anymore.
        return this.storage
                .exists(transactionMeta.getName(), timer.getRemaining())
                .thenComposeAsync(exists -> {
                    if (exists) {
                        return Futures.failedFuture(new ReconciliationFailureException(
                                String.format("Cannot reconcile operation '%s' because the transaction segment still exists in Storage.", op), this.metadata, storageInfo));
                    }

                    // Clear out any attributes.
                    return this.dataSource.deleteAllAttributes(transactionMeta, timer.getRemaining());
                }, this.executor)
                .thenApplyAsync(v -> {
                    // Reconciliation complete. Pop the first operation off the list and update the metadata for the transaction segment.
                    StorageOperation processedOperation = this.operations.removeFirst();
                    assert processedOperation != null && processedOperation instanceof MergeSegmentOperation : "First outstanding operation was not a MergeSegmentOperation";

                    int newCount = this.mergeTransactionCount.decrementAndGet();
                    assert newCount >= 0 : "Negative value for mergeTransactionCount";

                    // Since the operation is already reconciled, the StorageLength of this Segment must be at least
                    // the last offset of the operation. We are about to invoke ReadIndex.completeMerge(), which requires
                    // that this value be set to at least the last offset of the merged Segment, so we need to ensure it's
                    // set now. This will also be set at the end of reconciliation, but we cannot wait until then to invoke
                    // the callbacks.
                    long minStorageLength = processedOperation.getLastStreamSegmentOffset();
                    if (this.metadata.getStorageLength() < minStorageLength) {
                        this.metadata.setStorageLength(minStorageLength);
                    }

                    updateMetadataForTransactionPostMerger(transactionMeta, processedOperation.getStreamSegmentId());
                    return new WriterFlushResult().withMergedBytes(op.getLength());
                }, this.executor);
    }

    /**
     * Attempts to reconcile a StreamSegmentSealOperation.
     *
     * @param storageInfo The current state of the Segment in Storage.
     * @param timeout     Timeout for the operation.
     * @return A CompletableFuture containing a FlushResult with the number of bytes reconciled, or failed with a ReconciliationFailureException,
     * if the operation cannot be reconciled, based on the in-memory metadata or the current state of the Segment in Storage.
     */
    private CompletableFuture<WriterFlushResult> reconcileSealOperation(SegmentProperties storageInfo, Duration timeout) {
        // All we need to do is verify that the Segment is actually sealed in Storage. An exception to this rule is when
        // the segment has a length of 0, which means it may not have been created yet.
        if (storageInfo.isSealed() || storageInfo.getLength() == 0) {
            return CompletableFuture.supplyAsync(() -> {
                // Update metadata and the internal state (this also pops the first Op from the operation list).
                updateStatePostSeal();
                return new WriterFlushResult();
            }, this.executor);
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
     * * SegmentId matches this SegmentAggregator's SegmentId
     * * If Segment is Sealed, only TruncateSegmentOperations are allowed.
     * * If Segment is deleted, no further operations are allowed.
     *
     * @param operation The operation to check.
     * @throws IllegalArgumentException If any of the validations failed.
     */
    private void checkValidOperation(StorageOperation operation) throws DataCorruptionException {
        // Verify that the SegmentOperation has been routed to the correct SegmentAggregator instance.
        Preconditions.checkArgument(
                operation.getStreamSegmentId() == this.metadata.getId(),
                "Operation '%s' refers to a different Segment than this one (%s).", operation, this.metadata.getId());

        // After Sealing, we can only Truncate or Delete a Segment.
        if (this.hasSealPending.get() && !isTruncateOperation(operation) && !isDeleteOperation(operation)) {
            throw new DataCorruptionException(String.format("Illegal operation for a sealed Segment; received '%s'.", operation));
        }
    }

    /**
     * Validates that the given StorageOperation can be processed, given the current accumulated state of the Segment.
     *
     * @param operation The operation to check.
     * @throws DataCorruptionException  If any of the validations failed.
     * @throws IllegalArgumentException If the operation has an undefined Offset or Length (these are not considered data-
     *                                  corrupting issues).
     */
    private void checkValidStorageOperation(StorageOperation operation) throws DataCorruptionException {
        // StreamSegmentAppendOperations need to be pre-processed into CachedStreamSegmentAppendOperations.
        Preconditions.checkArgument(!(operation instanceof StreamSegmentAppendOperation), "SegmentAggregator cannot process StreamSegmentAppendOperations.");

        // Verify operation offset against the lastAddedOffset (whether the last Op in the list or StorageLength).
        long offset = operation.getStreamSegmentOffset();
        long length = operation.getLength();
        Preconditions.checkArgument(offset >= 0, "Operation '%s' has an invalid offset (%s).", operation, operation.getStreamSegmentOffset());
        Preconditions.checkArgument(length >= 0, "Operation '%s' has an invalid length (%s).", operation, operation.getLength());

        // Check that operations are contiguous (only for the operations after the first one - as we initialize lastAddedOffset on the first op).
        if (isTruncateOperation(operation)) {
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
     * @param flushData The arguments used for flushing.
     * @return A FlushResult containing statistics about the flush operation.
     */
    private WriterFlushResult updateStatePostFlush(BufferView flushData) {
        // Update the metadata Storage Length, if necessary.
        long newLength = this.metadata.getStorageLength();
        int flushLength = flushData == null ? 0 : flushData.getLength();
        if (flushLength > 0) {
            newLength += flushData.getLength();
            this.metadata.setStorageLength(newLength);
        }

        // Remove Append Operations from the outstanding list as long as every single byte they contain have been committed.
        boolean reachedEnd = false;
        while (this.operations.size() > 0 && !reachedEnd) {
            StorageOperation first = this.operations.getFirst();
            long lastOffset = first.getLastStreamSegmentOffset();
            reachedEnd = lastOffset >= newLength;
            if (!isAppendOperation(first)) {
                // We can only remove Append Operations.
                reachedEnd = true;
            } else if (lastOffset <= newLength) {
                // Fully flushed Append Operation.
                this.operations.removeFirst();
            }
        }

        // Update the last flush checkpoint.
        this.lastFlush.set(this.timer.getElapsed());
        return new WriterFlushResult().withFlushedBytes(flushLength);
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

    @SneakyThrows(StreamSegmentNotExistsException.class)
    private void updateMetadataForTransactionPostMerger(UpdateableSegmentMetadata transactionMetadata, long targetSegmentId) {
        // The other StreamSegment no longer exists and/or is no longer usable. Make sure it is marked as deleted.
        updateMetadataPostDeletion(transactionMetadata);

        // Complete the merger (in the ReadIndex and whatever other listeners we might have).
        this.dataSource.completeMerge(targetSegmentId, transactionMetadata.getId());
    }

    private void updateMetadataPostDeletion(UpdateableSegmentMetadata metadata) {
        metadata.markDeleted();
        metadata.markDeletedInStorage();
    }

    /**
     * Determines if the given StorageOperation is an Append Operation.
     *
     * @param op The operation to test.
     * @return True if an Append Operation (Cached or non-cached), false otherwise.
     */
    private boolean isAppendOperation(StorageOperation op) {
        return op instanceof AggregatedAppendOperation;
    }

    /**
     * Determines whether the given StorageOperation's Offset must be match the current expected Offset.
     */
    private boolean isTruncateOperation(StorageOperation operation) {
        return operation instanceof StreamSegmentTruncateOperation;
    }

    /**
     * Determines whether the given StorageOperation is a {@link DeleteSegmentOperation}.
     */
    private boolean isDeleteOperation(StorageOperation operation) {
        return operation instanceof DeleteSegmentOperation;
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
     * @param timeout     Timeout for the operation.
     * @return A Future that will contain information about the opened Segment.
     */
    private CompletableFuture<SegmentProperties> openWrite(String segmentName, AtomicReference<SegmentHandle> handleRef, Duration timeout) {
        return this.storage
                .openWrite(segmentName)
                .thenComposeAsync(handle -> {
                    handleRef.set(handle);
                    return this.storage.getStreamSegmentInfo(segmentName, timeout);
                }, this.executor);
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

        void reconcileComplete(int reconciledBytes) {
            this.streamSegmentOffset.addAndGet(reconciledBytes);
            this.length.addAndGet(-reconciledBytes);
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
        public String toString() {
            return String.format(
                "AggregatedAppend: SegmentId = %s, Offsets = [%s-%s), SeqNo = %s", getStreamSegmentId(),
                getStreamSegmentOffset(), getStreamSegmentOffset() + getLength(), getSequenceNumber());
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

        synchronized void clear() {
            this.queue.clear();
        }

        synchronized List<StorageOperation> filter(Predicate<StorageOperation> test) {
            return this.queue.stream().filter(test).collect(Collectors.toList());
        }
    }

    //endregion
}
