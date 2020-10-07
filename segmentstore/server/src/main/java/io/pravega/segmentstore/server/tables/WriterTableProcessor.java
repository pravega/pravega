/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.BufferView;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.WriterFlushResult;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * A {@link WriterSegmentProcessor} that handles the asynchronous indexing of Table Entries.
 */
@Slf4j
public class WriterTableProcessor implements WriterSegmentProcessor {
    //region Members

    private final TableWriterConnector connector;
    private final IndexWriter indexWriter;
    private final ScheduledExecutorService executor;
    private final OperationAggregator aggregator;
    private final AtomicLong lastAddedOffset;
    private final AtomicBoolean closed;
    private final String traceObjectId;
    private final TableCompactor compactor;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the WriterTableProcessor class.
     *
     * @param connector The {@link TableWriterConnector} that this instance will use to to access Table-related information.
     * @param executor  An Executor for async operations.
     */
    WriterTableProcessor(@NonNull TableWriterConnector connector, @NonNull ScheduledExecutorService executor) {
        this.connector = connector;
        this.executor = executor;
        this.indexWriter = new IndexWriter(connector.getKeyHasher(), executor);
        this.aggregator = new OperationAggregator(this.indexWriter.getLastIndexedOffset(this.connector.getMetadata()));
        this.lastAddedOffset = new AtomicLong(-1);
        this.closed = new AtomicBoolean();
        this.traceObjectId = String.format("TableProcessor[%d-%d]", this.connector.getMetadata().getContainerId(), this.connector.getMetadata().getId());
        this.compactor = new TableCompactor(connector, this.indexWriter, this.executor);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            this.connector.close();
            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region WriterSegmentProcessor Implementation

    @Override
    public void add(SegmentOperation operation) throws DataCorruptionException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkArgument(operation.getStreamSegmentId() == this.connector.getMetadata().getId(),
                "Operation '%s' refers to a different Segment than this one (%s).", operation, this.connector.getMetadata().getId());
        Preconditions.checkArgument(operation.getSequenceNumber() != Operation.NO_SEQUENCE_NUMBER,
                "Operation '%s' does not have a Sequence Number assigned.", operation);
        if (this.connector.getMetadata().isDeleted() || !(operation instanceof CachedStreamSegmentAppendOperation)) {
            // Segment is either deleted or this is not an append operation. Nothing for us to do here.
            return;
        }

        CachedStreamSegmentAppendOperation append = (CachedStreamSegmentAppendOperation) operation;
        if (this.lastAddedOffset.get() >= 0) {
            // We have processed at least one operation so far. Verify operations are contiguous.
            if (this.lastAddedOffset.get() != append.getStreamSegmentOffset()) {
                throw new DataCorruptionException(String.format("Wrong offset for Operation '%s'. Expected: %s, actual: %d.",
                        operation, this.lastAddedOffset, append.getStreamSegmentOffset()));
            }
        } else {
            // We haven't processed any operation so far (this is the first). Verify that we are resuming from an expected
            // offset and not skipping any updates.
            if (this.aggregator.getLastIndexedOffset() < append.getStreamSegmentOffset()) {
                throw new DataCorruptionException(String.format("Operation '%s' begins after TABLE_INDEXED_OFFSET. Expected: %s, actual: %d.",
                        operation, this.aggregator.getLastIndexedOffset(), append.getStreamSegmentOffset()));
            }
        }

        if (append.getStreamSegmentOffset() >= this.aggregator.getLastIndexedOffset()) {
            // Operation has not been indexed yet; add it to the internal list so we can process it.
            // NOTE: appends that contain more than one TableEntry (for batch updates) will be indexed atomically (either
            // all Table Entries are indexed or none), so it is safe to compare this with the first offset of the append.
            this.aggregator.add(append);
            this.lastAddedOffset.set(append.getLastStreamSegmentOffset());
            log.debug("{}: Add {} (State={}).", this.traceObjectId, operation, this.aggregator);
        } else {
            log.debug("{}: Skipped {} (State={}).", this.traceObjectId, operation, this.aggregator);
        }
    }

    @Override
    public boolean isClosed() {
        return this.closed.get();
    }

    @Override
    public long getLowestUncommittedSequenceNumber() {
        return this.aggregator.getFirstSequenceNumber();
    }

    @Override
    public boolean mustFlush() {
        if (this.connector.getMetadata().isDeleted()) {
            return false;
        }

        return !this.aggregator.isEmpty();
    }

    @Override
    public CompletableFuture<WriterFlushResult> flush(Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.connector
                .getSegment(timer.getRemaining())
                .thenComposeAsync(segment -> flushWithSingleRetry(segment, timer)
                                .thenComposeAsync(flushResult -> {
                                    flushComplete(flushResult.lastIndexedOffset);
                                    return compactIfNeeded(segment, flushResult.highestCopiedOffset, timer)
                                            .thenApply(v -> flushResult);
                                }, this.executor),
                        this.executor);
    }

    @Override
    public String toString() {
        return String.format(
                "[%d: %s] Count = %d, LastOffset = %s, LUSN = %d",
                this.connector.getMetadata().getId(),
                this.connector.getMetadata().getName(),
                this.aggregator.size(),
                this.lastAddedOffset,
                getLowestUncommittedSequenceNumber());
    }

    //endregion

    //region Helpers

    /**
     * Performs a Table Segment Compaction if needed.
     *
     * @param segment             The Segment to compact.
     * @param highestCopiedOffset The highest copied offset that was encountered during indexing. This is used to determine
     *                            where to safely truncate the segment, if at all.
     * @param timer               Timer for the operation.
     * @return A CompletableFuture that, when completed, will indicate the compaction (if anything) has completed. This
     * future will always complete normally; any exceptions are logged but not otherwise bubbled up.
     */
    private CompletableFuture<Void> compactIfNeeded(DirectSegmentAccess segment, long highestCopiedOffset, TimeoutTimer timer) {
        // Decide if compaction is needed. If not, bail out early.
        SegmentProperties info = segment.getInfo();

        CompletableFuture<Void> result;
        if (this.compactor.isCompactionRequired(info)) {
            result = this.compactor.compact(segment, timer);
        } else {
            log.debug("{}: No compaction required at this time.", this.traceObjectId);
            result = CompletableFuture.completedFuture(null);
        }

        return result
                .thenComposeAsync(v -> {
                    // Calculate the safe truncation offset.
                    long truncateOffset = this.compactor.calculateTruncationOffset(segment.getInfo(), highestCopiedOffset);

                    // Truncate if necessary.
                    if (truncateOffset > 0) {
                        log.debug("{}: Truncating segment at offset {}.", this.traceObjectId, truncateOffset);
                        return segment.truncate(truncateOffset, timer.getRemaining());
                    } else {
                        log.debug("{}: No segment truncation possible now.", this.traceObjectId);
                        return CompletableFuture.completedFuture(null);
                    }
                }, this.executor)
                .exceptionally(ex -> {
                    // We want to record the compaction failure, but since this is not a critical step in making progress,
                    // we do not want to prevent the StorageWriter from ack-ing operations.
                    log.error("{}: Compaction failed.", this.traceObjectId, ex);
                    return null;
                });
    }


    /**
     * Performs a flush attempt, and retries it in case it failed with {@link BadAttributeUpdateException} for the
     * {@link TableAttributes#INDEX_OFFSET} attribute.
     *
     * In case of a retry, it reconciles the cached value for the {@link TableAttributes#INDEX_OFFSET} attribute and
     * updates any internal state.
     *
     * @param segment A {@link DirectSegmentAccess} representing the Segment to flush on.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will indicate the flush has completed successfully. If the
     * operation failed, it will be failed with the appropriate exception. Notable exceptions:
     * <ul>
     * <li>{@link BadAttributeUpdateException} If a conditional update on the {@link TableAttributes#INDEX_OFFSET} attribute
     * failed (for the second attempt) or on any other attribute failed (for any attempt).
     * <li>{@link DataCorruptionException} If the reconciliation failed (in which case no flush is attempted.
     * </ul>
     */
    private CompletableFuture<TableWriterFlushResult> flushWithSingleRetry(DirectSegmentAccess segment, TimeoutTimer timer) {
        return Futures.exceptionallyComposeExpecting(
                flushOnce(segment, timer),
                this::canRetryFlushException,
                () -> {
                    reconcileTableIndexOffset();
                    return flushOnce(segment, timer);
                });
    }

    /**
     * Updates the internal state post flush and notifies the {@link TableWriterConnector} of the fact.
     *
     * @param lastIndexedOffset The last offset in the Table Segment that was indexed.
     */
    private void flushComplete(long lastIndexedOffset) {
        this.aggregator.reset();
        this.aggregator.setLastIndexedOffset(lastIndexedOffset);
        this.connector.notifyIndexOffsetChanged(this.aggregator.getLastIndexedOffset());
        log.debug("{}: FlushComplete (State={}).", this.traceObjectId, this.aggregator);
    }

    /**
     * Performs a single flush attempt.
     *
     * @param segment A {@link DirectSegmentAccess} representing the Segment to flush on.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will indicate the flush has completed successfully. If the
     * operation failed, it will be failed with the appropriate exception. Notable exceptions:
     * <ul>
     * <li>{@link BadAttributeUpdateException} If a conditional update on the {@link TableAttributes#INDEX_OFFSET} attribute failed.
     * </ul>
     */
    private CompletableFuture<TableWriterFlushResult> flushOnce(DirectSegmentAccess segment, TimeoutTimer timer) {
        // Index all the keys in the segment range pointed to by the aggregator.
        KeyUpdateCollection keyUpdates = readKeysFromSegment(segment, this.aggregator.getFirstOffset(), this.aggregator.getLastOffset(), timer);
        log.debug("{}: Flush.ReadFromSegment {} UpdateKeys(s).", this.traceObjectId, keyUpdates.getUpdates().size());

        // Group keys by their assigned TableBucket (whether existing or not), then fetch all existing keys
        // for each such bucket and finally (reindex) update the bucket.
        return this.indexWriter
                .groupByBucket(segment, keyUpdates.getUpdates(), timer)
                .thenComposeAsync(builders -> fetchExistingKeys(builders, segment, timer)
                                .thenComposeAsync(v -> {
                                    val bucketUpdates = builders.stream().map(BucketUpdate.Builder::build).collect(Collectors.toList());
                                    logBucketUpdates(bucketUpdates);
                                    return this.indexWriter.updateBuckets(segment, bucketUpdates,
                                            this.aggregator.getLastIndexedOffset(), keyUpdates.getLastIndexedOffset(),
                                            keyUpdates.getTotalUpdateCount(), timer.getRemaining());
                                }, this.executor),
                        this.executor)
                .thenApply(ignored -> new TableWriterFlushResult(keyUpdates.getLastIndexedOffset(), keyUpdates.getHighestCopiedOffset()));
    }

    @SneakyThrows(DataCorruptionException.class)
    private void reconcileTableIndexOffset() {
        long tableIndexOffset = this.indexWriter.getLastIndexedOffset(this.connector.getMetadata());
        if (tableIndexOffset < this.aggregator.getLastIndexedOffset()) {
            // This should not happen, ever!
            throw new DataCorruptionException(String.format("Cannot reconcile INDEX_OFFSET attribute (%s) for Segment '%s'. "
                            + "It is lower than our known value (%s).",
                    tableIndexOffset, this.connector.getMetadata().getId(), this.aggregator.getLastIndexedOffset()));
        }

        if (!this.aggregator.setLastIndexedOffset(tableIndexOffset)) {
            throw new DataCorruptionException(String.format("Cannot reconcile INDEX_OFFSET attribute (%s) for Segment '%s'. "
                            + "Most likely it does not conform to an append boundary.  Existing value: %s.",
                    tableIndexOffset, this.connector.getMetadata().getId(), this.aggregator.getLastIndexedOffset()));
        }

        log.info("{}: ReconcileTableIndexOffset (State={}).", this.traceObjectId, this.aggregator);
    }

    private boolean canRetryFlushException(Throwable ex) {
        if (ex instanceof BadAttributeUpdateException) {
            val bau = (BadAttributeUpdateException) ex;
            return bau.getAttributeId() != null && bau.getAttributeId().equals(TableAttributes.INDEX_OFFSET);
        }

        return false;
    }

    /**
     * Reads all the Keys from the given Segment between the given offsets and indexes them by key.
     *
     * @param segment     The InputStream to process.
     * @param firstOffset The first offset in the Segment to start reading Keys at.
     * @param lastOffset  The last offset in the Segment to read Keys until.
     * @param timer       Timer for the operation.
     * @return A {@link KeyUpdateCollection}s containing the indexed keys.
     */
    @SneakyThrows(IOException.class)
    private KeyUpdateCollection readKeysFromSegment(DirectSegmentAccess segment, long firstOffset, long lastOffset, TimeoutTimer timer) {
        KeyUpdateCollection keyUpdates = new KeyUpdateCollection();
        try (InputStream input = readFromInMemorySegment(segment, firstOffset, lastOffset, timer).getReader()) {
            long segmentOffset = firstOffset;
            while (segmentOffset < lastOffset) {
                segmentOffset += indexSingleKey(input, segmentOffset, keyUpdates);
            }
        }
        return keyUpdates;
    }

    /**
     * Indexes a single Key for a Table Entry that begins with the first byte of the given InputStream.
     *
     * @param input         The InputStream that contains the Table Entry to index.
     * @param entryOffset   The offset within the Segment where this Table Entry begins.
     * @param keyUpdateCollection A Map where to add the result.
     * @return The number of bytes processed from the given InputStream.
     * @throws IOException If an IOException occurred.
     */
    private int indexSingleKey(InputStream input, long entryOffset, KeyUpdateCollection keyUpdateCollection) throws IOException {
        // Retrieve the next entry, get its Key and hash it.
        val e = AsyncTableEntryReader.readEntryComponents(input, entryOffset, this.connector.getSerializer());
        HashedArray key = new HashedArray(e.getKey());

        // Index the Key. If it was used before, then their versions will be compared to determine which one prevails.
        val update = new BucketUpdate.KeyUpdate(key, entryOffset, e.getVersion(), e.getHeader().isDeletion());
        keyUpdateCollection.add(update, e.getHeader().getTotalLength(), e.getHeader().getEntryVersion());
        return e.getHeader().getTotalLength();
    }

    /**
     * Reads from the Segment between the given offsets. This method assumes all the data is readily available in the cache,
     * otherwise it will block synchronously for Storage retrieval.
     *
     * @param segment     The Segment to read from.
     * @param startOffset The offset to start reading from.
     * @param endOffset   The offset to stop reading at.
     * @param timer       Timer for the operation.
     * @return A {@link BufferView} with the requested data.
     */
    private BufferView readFromInMemorySegment(DirectSegmentAccess segment, long startOffset, long endOffset, TimeoutTimer timer) {
        long readOffset = startOffset;
        long remainingLength = endOffset - startOffset;
        ArrayList<BufferView> inputs = new ArrayList<>();
        while (remainingLength > 0) {
            int readLength = (int) Math.min(remainingLength, Integer.MAX_VALUE);
            try (ReadResult readResult = segment.read(readOffset, readLength, timer.getRemaining())) {
                inputs.addAll(readResult.readRemaining(readLength, timer.getRemaining()));
                assert readResult.getConsumedLength() == readLength : "Expecting a full read (from memory).";
                remainingLength -= readResult.getConsumedLength();
                readOffset += readResult.getConsumedLength();
            }
        }

        return BufferView.wrap(inputs);
    }

    /**
     * Fetches the existing keys for all buckets in the given collection of {@link BucketUpdate}s.
     *
     * @param builders A Collection of {@link BucketUpdate.Builder}s to fetch for. Upon completion of this method,
     *                      this will be updated with the existing keys.
     * @param segment       The segment to operate on.
     * @param timer         Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the collection of {@link BucketUpdate}s that was
     * passed in, with the existing keys populated.
     */
    private CompletableFuture<Void> fetchExistingKeys(Collection<BucketUpdate.Builder> builders, DirectSegmentAccess segment,
                                                      TimeoutTimer timer) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return Futures.loop(
                builders,
                bucketUpdate -> fetchExistingKeys(bucketUpdate, segment, timer).thenApply(v -> true),
                this.executor);
    }

    /**
     * Fetches the existing keys for the given {@link BucketUpdate}.
     *
     * @param builder The BucketUpdate to fetch keys for. Upon completion of this method, this will be updated with
     *                the existing keys.
     * @param segment The segment to operate on.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation is done.
     */
    private CompletableFuture<Void> fetchExistingKeys(BucketUpdate.Builder builder, DirectSegmentAccess segment, TimeoutTimer timer) {
        // Get all Key locations, using the bucket's last offset and backpointers.
        return TableBucketReader
                .key(segment, this.indexWriter::getBackpointerOffset, this.executor)
                .findAll(builder.getBucket().getSegmentOffset(),
                        (key, offset) -> builder.withExistingKey(new BucketUpdate.KeyInfo(new HashedArray(key.getKey()), offset, key.getVersion())),
                        timer);

    }

    private void logBucketUpdates(Collection<BucketUpdate> bucketUpdates) {
        if (!log.isTraceEnabled()) {
            // Do not bother to generate log messages if TRACE is not enabled.
            return;
        }
        log.trace("{}: Updating {} TableBucket(s).", this.traceObjectId, bucketUpdates.size());
        bucketUpdates.forEach(bu -> log.trace("{}: TableBucket [Offset={}, {}]: ExistingKeys=[{}], Updates=[{}].",
                this.traceObjectId,
                bu.getBucketOffset(),
                bu.getBucket(),
                bu.getExistingKeys().stream().map(Object::toString).collect(Collectors.joining("; ")),
                bu.getKeyUpdates().stream().map(Object::toString).collect(Collectors.joining("; ")))
        );
    }

    //endregion

    //region Helper Classes

    @ThreadSafe
    private static class OperationAggregator {
        @GuardedBy("this")
        private long firstSeqNo;
        @GuardedBy("this")
        private long lastOffset;
        @GuardedBy("this")
        private long lastIndexedOffset;
        @GuardedBy("this")
        private final ArrayList<Long> appendOffsets;

        OperationAggregator(long lastIndexedOffset) {
            this.appendOffsets = new ArrayList<>();
            reset();
            this.lastIndexedOffset = lastIndexedOffset;
        }

        synchronized void reset() {
            this.firstSeqNo = Operation.NO_SEQUENCE_NUMBER;
            this.lastOffset = -1;
            this.appendOffsets.clear();
        }

        synchronized void add(CachedStreamSegmentAppendOperation op) {
            if (this.appendOffsets.size() == 0) {
                this.firstSeqNo = op.getSequenceNumber();
            }

            this.lastOffset = op.getLastStreamSegmentOffset();
            this.appendOffsets.add(op.getStreamSegmentOffset());
        }

        synchronized boolean isEmpty() {
            return this.appendOffsets.isEmpty();
        }

        synchronized long getFirstSequenceNumber() {
            return this.firstSeqNo;
        }

        synchronized long getFirstOffset() {
            return this.appendOffsets.size() == 0 ? -1 : this.appendOffsets.get(0);
        }

        synchronized long getLastOffset() {
            return this.lastOffset;
        }

        synchronized long getLastIndexedOffset() {
            return this.lastIndexedOffset;
        }

        synchronized boolean setLastIndexedOffset(long value) {
            if (this.appendOffsets.size() > 0) {
                if (value >= getLastOffset()) {
                    // Clear everything - anyway we do not have enough info to determine if this is valid or not.
                    reset();
                } else {
                    // First, make sure we set this to a valid value.
                    int index = this.appendOffsets.indexOf(value);
                    if (index < 0) {
                        return false;
                    }

                    // Clear out smaller offsets.
                    this.appendOffsets.subList(0, index).clear();
                }
            }

            if (value >= 0) {
                this.lastIndexedOffset = value;
            }

            return true;
        }

        synchronized int size() {
            return this.appendOffsets.size();
        }

        @Override
        public synchronized String toString() {
            return String.format("Count = %d, FirstSN = %d, FirstOffset = %d, LastOffset = %d, LIdx = %s",
                    this.appendOffsets.size(), this.firstSeqNo, getFirstOffset(), getLastOffset(), getLastIndexedOffset());
        }
    }

    @RequiredArgsConstructor
    private static class TableWriterFlushResult extends WriterFlushResult {
        final long lastIndexedOffset;
        final long highestCopiedOffset;
    }

    //endregion
}
