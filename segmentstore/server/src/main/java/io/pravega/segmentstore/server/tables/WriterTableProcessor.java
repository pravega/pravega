/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.pravega.common.Exceptions;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.StreamHelpers;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.SegmentOperation;
import io.pravega.segmentstore.server.WriterFlushResult;
import io.pravega.segmentstore.server.WriterSegmentProcessor;
import io.pravega.segmentstore.server.logs.operations.CachedStreamSegmentAppendOperation;
import io.pravega.segmentstore.server.logs.operations.Operation;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Getter;
import lombok.NonNull;
import lombok.SneakyThrows;
import org.apache.commons.io.IOUtils;

/**
 * A {@link WriterSegmentProcessor} that handles the asynchronous indexing of Table Entries.
 */
public class WriterTableProcessor implements WriterSegmentProcessor {
    //region Members

    private final SegmentMetadata metadata;
    private final EntrySerializer serializer;
    private final IndexWriter indexWriter;
    private final GetSegment getSegment;
    private final ScheduledExecutorService executor;
    private final OperationAggregator aggregator;
    private final AtomicLong lastAddedOffset;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the WriterTableProcessor class.
     *
     * @param segmentMetadata The {@link SegmentMetadata} of the Segment this processor handles.
     * @param serializer      The {@link EntrySerializer} used to read Table Entries from the Segment.
     * @param hasher          The {@link KeyHasher} used to hash Table Keys.
     * @param getSegment      A Function that, when invoked with the name of a Segment, returns a {@link CompletableFuture}
     *                        containing a {@link DirectSegmentAccess} for that Segment.
     * @param executor        An Executor for async operations.
     */
    WriterTableProcessor(@NonNull SegmentMetadata segmentMetadata, @NonNull EntrySerializer serializer, @NonNull KeyHasher hasher,
                         @NonNull GetSegment getSegment, @NonNull ScheduledExecutorService executor) {
        this.metadata = segmentMetadata;
        this.serializer = serializer;
        this.getSegment = getSegment;
        this.executor = executor;
        this.indexWriter = new IndexWriter(hasher, executor);
        this.aggregator = new OperationAggregator(this.indexWriter.getLastIndexedOffset(segmentMetadata));
        this.lastAddedOffset = new AtomicLong(-1);
        this.closed = new AtomicBoolean();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.closed.set(true);
    }

    //endregion

    //region WriterSegmentProcessor Implementation

    @Override
    public void add(SegmentOperation operation) throws DataCorruptionException {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkArgument(operation.getStreamSegmentId() == this.metadata.getId(),
                "Operation '%s' refers to a different Segment than this one (%s).", operation, this.metadata.getId());
        Preconditions.checkArgument(operation.getSequenceNumber() != Operation.NO_SEQUENCE_NUMBER,
                "Operation '%s' does not have a Sequence Number assigned.", operation);
        if (this.metadata.isDeleted() || !(operation instanceof CachedStreamSegmentAppendOperation)) {
            // Segment is either deleted or this is not an append operation. Nothing for us to do here.
            return;
        }

        CachedStreamSegmentAppendOperation append = (CachedStreamSegmentAppendOperation) operation;
        if (this.lastAddedOffset.get() >= 0 && this.lastAddedOffset.get() != append.getStreamSegmentOffset()) {
            throw new DataCorruptionException(String.format("Wrong offset for Operation '%s'. Expected: %s, actual: %d.",
                    operation, this.lastAddedOffset, append.getStreamSegmentOffset()));
        }

        if (append.getStreamSegmentOffset() >= this.aggregator.getLastIndexedOffset()) {
            // Operation has not been indexed yet; add it to the internal list so we can process it.
            // NOTE: appends that contain more than one TableEntry (for batch updates) will be indexed atomically (either
            // all Table Entries are indexed or none), so it is safe to compare this with the first offset of the append.
            this.aggregator.add(append);
            this.lastAddedOffset.set(append.getLastStreamSegmentOffset());
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
        if (this.metadata.isDeleted()) {
            return false;
        }

        return !this.aggregator.isEmpty();
    }

    @Override
    public CompletableFuture<WriterFlushResult> flush(Duration timeout) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return this.getSegment
                .apply(this.metadata.getName(), timer.getRemaining())
                .thenComposeAsync(segment -> flush(segment, timer), this.executor)
                .thenApply(v -> {
                    // We're done processing. Reset the aggregator.
                    this.aggregator.reset();
                    this.aggregator.setLastIndexedOffset(this.indexWriter.getLastIndexedOffset(this.metadata));
                    return new WriterFlushResult();
                });

    }

    private CompletableFuture<?> flush(DirectSegmentAccess segment, TimeoutTimer timer) {
        // Index all the keys in the segment range pointed to by the aggregator.
        KeyUpdateCollection keyUpdates = readKeysFromSegment(segment, this.aggregator.getFirstOffset(), this.aggregator.getLastOffset(), timer);

        // Group keys by their assigned TableBucket (whether existing or not), then fetch all existing keys
        // for each such bucket and finally (reindex) update the bucket.
        return this.indexWriter
                .groupByBucket(keyUpdates.getUpdates(), segment, timer)
                .thenComposeAsync(bucketUpdates -> fetchExistingKeys(bucketUpdates, segment, timer)
                                .thenComposeAsync(v -> this.indexWriter.updateBuckets(bucketUpdates, segment,
                                        this.aggregator.getLastIndexedOffset(), keyUpdates.getLastIndexedOffset(), timer.getRemaining()),
                                        this.executor),
                        this.executor);
    }

    //endregion

    //region Helpers

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
        try (InputStream input = readFromInMemorySegment(segment, firstOffset, lastOffset, timer)) {
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
        EntrySerializer.Header h = this.serializer.readHeader(input);
        HashedArray key = new HashedArray(StreamHelpers.readAll(input, h.getKeyLength()));

        // Index the Key. If it was used before, it must have had a lower offset, so this supersedes it.
        keyUpdateCollection.add(new KeyUpdate(key, entryOffset, h.isDeletion()), h.getTotalLength());

        // We don't care about the value; so skip over it.
        if (h.getValueLength() > 0) {
            IOUtils.skipFully(input, h.getValueLength());
        }

        return h.getTotalLength();
    }

    /**
     * Reads from the Segment between the given offsets. This method assumes all the data is readily available in the cache,
     * otherwise it will block synchronously for Storage retrieval.
     *
     * @param segment     The Segment to read from.
     * @param startOffset The offset to start reading from.
     * @param endOffset   The offset to stop reading at.
     * @param timer       Timer for the operation.
     * @return An Enumeration of InputStreams representing the read data.
     */
    private InputStream readFromInMemorySegment(DirectSegmentAccess segment, long startOffset, long endOffset, TimeoutTimer timer) {
        long readOffset = startOffset;
        long remainingLength = endOffset - startOffset;
        ArrayList<InputStream> inputs = new ArrayList<>();
        while (remainingLength > 0) {
            int readLength = (int) Math.min(remainingLength, Integer.MAX_VALUE);
            try (ReadResult readResult = segment.read(readOffset, readLength, timer.getRemaining())) {
                inputs.addAll(readResult.readRemaining(readLength, timer.getRemaining()));
                assert readResult.getConsumedLength() == readLength : "Expecting a full read (from memory).";
                remainingLength -= readResult.getConsumedLength();
                readOffset += readResult.getConsumedLength();
            }
        }

        return new SequenceInputStream(Iterators.asEnumeration(inputs.iterator()));
    }

    /**
     * Fetches the existing keys for all buckets in the given collection of {@link BucketUpdate}s.
     *
     * @param bucketUpdates The BucketUpdateCollection to fetch for. Upon completion of this method, this will be updated
     *                      with the existing keys.
     * @param segment       The segment to operate on.
     * @param timer         Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the collection of {@link BucketUpdate}s that was
     * passed in, with the existing keys populated.
     */
    private CompletableFuture<Void> fetchExistingKeys(Collection<BucketUpdate> bucketUpdates, DirectSegmentAccess segment, TimeoutTimer timer) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return Futures.loop(
                bucketUpdates,
                bucketUpdate -> fetchExistingKeys(bucketUpdate, segment, timer).thenApply(v -> true),
                this.executor);
    }

    /**
     * Fetches the existing keys for the given {@link BucketUpdate}.
     *
     * @param bucketUpdate The BucketUpdate to fetch keys for. Upon completion of this method, this will be updated with
     *                     the existing keys.
     * @param segment      The segment to operate on.
     * @param timer        Timer for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation is done.
     */
    private CompletableFuture<Void> fetchExistingKeys(BucketUpdate bucketUpdate, DirectSegmentAccess segment, TimeoutTimer timer) {
        if (bucketUpdate.getBucket().isPartial()) {
            // Incomplete bucket, so it can't have any data.
            return CompletableFuture.completedFuture(null);
        }

        // Get all Key locations, using the bucket's last offset and backpointers.
        final int maxReadLength = EntrySerializer.HEADER_LENGTH + EntrySerializer.MAX_KEY_LENGTH;
        AtomicLong offset = new AtomicLong(this.indexWriter.getOffset(bucketUpdate.getBucket().getLastNode()));
        return Futures.loop(
                () -> offset.get() >= 0,
                () -> {
                    // Read the Key from the Segment.
                    ReadResult readResult = segment.read(offset.get(), maxReadLength, timer.getRemaining());
                    AsyncTableKeyBuilder builder = new AsyncTableKeyBuilder(this.serializer, timer);
                    AsyncReadResultProcessor.process(readResult, builder, this.executor);
                    return builder.getResult()
                                  .thenComposeAsync(keyView -> {
                                      // Record the Key and its location.
                                      bucketUpdate.withExistingKey(new KeyInfo(new HashedArray(keyView), offset.get()));

                                      // Get the next Key Location for this bucket.
                                      return this.indexWriter.getBackpointerOffset(offset.get(), segment, timer.getRemaining());
                                  }, this.executor);
                },
                offset::set,
                this.executor);
    }

    //endregion

    //region Helper Classes

    @ThreadSafe
    private static class OperationAggregator {
        @GuardedBy("this")
        private long firstSeqNo;
        @GuardedBy("this")
        private long firstOffset;
        @GuardedBy("this")
        private long lastOffset;
        @GuardedBy("this")
        private int count;
        @GuardedBy("this")
        private long lastIndexedOffset;

        OperationAggregator(long lastIndexedOffset) {
            reset();
            this.lastIndexedOffset = lastIndexedOffset;
        }

        synchronized void reset() {
            this.firstSeqNo = Operation.NO_SEQUENCE_NUMBER;
            this.firstOffset = -1;
            this.lastOffset = -1;
            this.count = 0;
        }

        synchronized void add(CachedStreamSegmentAppendOperation op) {
            if (this.count == 0) {
                this.firstSeqNo = op.getSequenceNumber();
                this.firstOffset = op.getStreamSegmentOffset();
            }

            this.lastOffset = op.getLastStreamSegmentOffset();
            this.count++;
        }

        synchronized boolean isEmpty() {
            return this.count == 0;
        }

        synchronized long getFirstSequenceNumber() {
            return this.firstSeqNo;
        }

        synchronized long getFirstOffset() {
            return this.firstOffset;
        }

        synchronized long getLastOffset() {
            return this.lastOffset;
        }

        synchronized long getLastIndexedOffset() {
            return this.lastIndexedOffset;
        }

        synchronized void setLastIndexedOffset(long value) {
            this.lastIndexedOffset = value;
        }

        @Override
        public synchronized String toString() {
            return String.format("Count = %d, FirstSN = %d, FirstOffset = %d, LastOffset = %d",
                    this.count, this.firstSeqNo, this.firstOffset, this.lastOffset);
        }
    }

    /**
     * Collection of Keys to their superseding {@link KeyUpdate}s.
     */
    @NotThreadSafe
    private static class KeyUpdateCollection {
        private final HashMap<HashedArray, KeyUpdate> updates = new HashMap<>();

        /**
         * The Segment offset before which every single byte has been indexed (i.e., the last offset of the last update).
         */
        @Getter
        private long lastIndexedOffset = 0L;

        void add(KeyUpdate update, int entryLength) {
            this.updates.put(update.getKey(), update);
            long lastOffset = update.getOffset() + entryLength;
            if (lastOffset > this.lastIndexedOffset) {
                this.lastIndexedOffset = lastOffset;
            }
        }

        Collection<KeyUpdate> getUpdates() {
            return this.updates.values();
        }
    }

    @FunctionalInterface
    public interface GetSegment {
        CompletableFuture<DirectSegmentAccess> apply(String segmentName, Duration timeout);
    }

    //endregion
}
