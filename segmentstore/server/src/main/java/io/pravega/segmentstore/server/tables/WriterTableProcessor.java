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
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.Attributes;
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
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.io.IOUtils;

/**
 * A {@link WriterSegmentProcessor} that handles the asynchronous indexing of Table Entries.
 */
public class WriterTableProcessor implements WriterSegmentProcessor {
    //region Members

    private final SegmentMetadata metadata;
    private final EntrySerializer serializer;
    private final KeyHasher hasher;
    private final Indexer indexer;
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
     * @param indexer         An {@link Indexer} that can be used to look up Table Buckets and generate update instructions
     *                        (in the form of {@link AttributeUpdate}s) for Table Entries in this Segment.
     * @param getSegment      A Function that, when invoked with the name of a Segment, returns a {@link CompletableFuture}
     *                        containing a {@link DirectSegmentAccess} for that Segment.
     * @param executor        An Executor for async operations.
     */
    WriterTableProcessor(@NonNull SegmentMetadata segmentMetadata, @NonNull EntrySerializer serializer, @NonNull KeyHasher hasher,
                         @NonNull Indexer indexer, @NonNull GetSegment getSegment, @NonNull ScheduledExecutorService executor) {
        this.metadata = segmentMetadata;
        this.serializer = serializer;
        this.hasher = hasher;
        this.indexer = indexer;
        this.getSegment = getSegment;
        this.executor = executor;
        this.aggregator = new OperationAggregator(this.indexer.getLastIndexedOffset(segmentMetadata));
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
        WriterFlushResult result = new WriterFlushResult();
        return this.getSegment
                .apply(this.metadata.getName(), timer.getRemaining())
                .thenComposeAsync(segment -> {
                    // Index all the keys in the segment range pointed to by the aggregator.
                    KeyUpdateCollection indexedKeys = readKeysFromSegment(segment, this.aggregator.getFirstOffset(), this.aggregator.getLastOffset(), timer);

                    // Group keys by their assigned TableBucket (whether existing or not), then fetch all existing keys
                    // for each such bucket and finally (reindex) update the bucket.
                    return groupByBucket(indexedKeys, segment, timer)
                            .thenComposeAsync(indexedBuckets -> getExistingKeys(indexedBuckets, segment, timer), this.executor)
                            .thenComposeAsync(indexedBuckets -> {
                                List<AttributeUpdate> updates = generateAttributeUpdates(indexedBuckets, indexedKeys.getLastIndexedOffset());
                                result.withFlushedAttributes(updates.size());
                                return updates.isEmpty()
                                        ? CompletableFuture.completedFuture(null)
                                        : segment.updateAttributes(updates, timer.getRemaining());
                            }, this.executor);
                }, this.executor)
                .thenApply(v -> {
                    // We're done processing. Reset the aggregator.
                    this.aggregator.reset();
                    this.aggregator.setLastIndexedOffset(this.indexer.getLastIndexedOffset(this.metadata));
                    return result;
                });

    }

    //endregion

    //region Helpers

    /**
     * Reads all the Keys from the given Segment between the given offsets and indexes them by key.
     *
     * @param segment     The InputStream to process.
     * @param firstOffset The first offset in the Segment to start reading Keys at.
     * @param lastOffset  The last offset in the Segment to read Keys at.
     * @param timer Timer for the operation.
     * @return A Map of {@link HashedArray}s (Keys) to {@link KeyUpdate}s (last update/delete for that key).
     */
    @SneakyThrows(IOException.class)
    private KeyUpdateCollection readKeysFromSegment(DirectSegmentAccess segment, long firstOffset, long lastOffset, TimeoutTimer timer) {
        KeyUpdateCollection indexedKeys = new KeyUpdateCollection();
        long segmentOffset = firstOffset;
        try (InputStream input = readFromInMemorySegment(segment, firstOffset, lastOffset, timer)) {
            while (segmentOffset < lastOffset) {
                segmentOffset += indexSingleKey(input, segmentOffset, indexedKeys);
            }
        }

        // Keep track of the last offset that we indexed.
        indexedKeys.setLastIndexedOffset(segmentOffset);
        return indexedKeys;
    }

    /**
     * Indexes a single Key for a Table Entry that begins with the first byte of the given InputStream.
     *
     * @param input         The InputStream that contains the Table Entry to index.
     * @param entryOffset   The offset within the Segment where this Table Entry begins.
     * @param keyUpdateCollection A Map where to add the result.
     * @throws IOException If an IOException occurred.
     */
    private int indexSingleKey(InputStream input, long entryOffset, KeyUpdateCollection keyUpdateCollection) throws IOException {
        // Retrieve the next entry, get its Key and hash it.
        EntrySerializer.Header h = this.serializer.readHeader(input);
        HashedArray key = new HashedArray(StreamHelpers.readAll(input, h.getKeyLength()));

        // Index the Key. If it was used before, it must have had a lower offset, so this supersedes it.
        keyUpdateCollection.put(key, new KeyUpdate(key, entryOffset, h.isDeletion()));

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
     * Groups the given {@link KeyUpdate}s by their associated buckets. These buckets may be partial (i.e., only part of
     * the hash matched) or a full match.
     *
     * @param keyUpdates A {@link KeyUpdateCollection} to index.
     * @param segment    The Segment to read from.
     * @param timer      Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the a collection of {@link BucketUpdate}s.
     */
    private CompletableFuture<Collection<BucketUpdate>> groupByBucket(KeyUpdateCollection keyUpdates, DirectSegmentAccess segment, TimeoutTimer timer) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        val result = new HashMap<TableBucket, BucketUpdate>();

        return Futures.loop(
                keyUpdates.entrySet(),
                item -> {
                    // Locate the Key's Bucket using the key's Hash.
                    KeyHash hash = this.hasher.hash(item.getKey().getArray());
                    return this.indexer
                            .locateBucket(segment, hash, timer)
                            .thenApply(bucket -> {
                                // Add the bucket to the result and record this Key as a "new" key in it.
                                BucketUpdate bu = result.computeIfAbsent(bucket, BucketUpdate::new);
                                bu.updatedKeys.put(item.getKey(), item.getValue());
                                return true;
                            });
                }, this.executor)
                .thenApply(v -> result.values());
    }

    /**
     * Fetches the existing keys for all buckets in the given collection of {@link BucketUpdate}s.
     *
     * @param bucketUpdateCollection The BucketUpdateCollection to fetch for. Upon completion of this method, this will be updated with the
     *                               existing keys.
     * @param segment                The segment to operate on.
     * @param timer                  Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain the collection of {@link BucketUpdate}s that was passed in, with
     * the existing keys populated.
     */
    private CompletableFuture<Collection<BucketUpdate>> getExistingKeys(Collection<BucketUpdate> bucketUpdateCollection, DirectSegmentAccess segment, TimeoutTimer timer) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        return Futures.loop(
                bucketUpdateCollection,
                bucketUpdate -> getExistingKeys(bucketUpdate, segment, timer).thenApply(v -> true),
                this.executor)
                .thenApply(v -> bucketUpdateCollection);
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
    private CompletableFuture<Void> getExistingKeys(BucketUpdate bucketUpdate, DirectSegmentAccess segment, TimeoutTimer timer) {
        final int maxReadLength = EntrySerializer.HEADER_LENGTH + EntrySerializer.MAX_KEY_LENGTH;
        TableBucket.Node lastNode = bucketUpdate.bucket.getLastNode();
        if (lastNode == null || lastNode.isIndexNode()) {
            // Incomplete bucket, so it can't have any data.
            return CompletableFuture.completedFuture(null);
        }

        // Get all Key locations, using the bucket's last offset and backpointers.
        // Get all the Keys from the segment by reading from it.
        // Add them to the result.
        AtomicLong offset = new AtomicLong(this.indexer.getOffset(lastNode));
        return Futures.loop(
                () -> offset.get() >= 0,
                () -> {
                    ReadResult readResult = segment.read(offset.get(), maxReadLength, timer.getRemaining());
                    AsyncTableKeyBuilder builder = new AsyncTableKeyBuilder(this.serializer, timer);
                    AsyncReadResultProcessor.process(readResult, builder, this.executor);
                    return builder.getResult()
                            .thenComposeAsync(keyView -> {
                                HashedArray key = new HashedArray(keyView.getCopy());
                                bucketUpdate.existingKeys.put(key, new KeyUpdate(key, offset.get(), false));
                                return this.indexer.getBackpointerOffset(segment, offset.get(), timer.getRemaining())
                                        .thenAccept(offset::set);
                            }, this.executor);
                },
                this.executor);
    }

    /**
     * Generates a List of {@link AttributeUpdate}s that, when applied to the given Segment, will update the Table Index
     * with the effect of adding the new keys.
     *
     * @param bucketUpdateCollection The collection of {@link BucketUpdate}s to generate for.
     * @param lastIndexedOffset      The last offset in the Segment that has been indexed so far.
     * @return A List of {@link AttributeUpdate}s.
     */
    private List<AttributeUpdate> generateAttributeUpdates(Collection<BucketUpdate> bucketUpdateCollection, long lastIndexedOffset) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        // Atomically update the Last Indexed Offset in the Segment's metadata, once we apply these changes.
        List<AttributeUpdate> attributeUpdates = new ArrayList<>();
        if (!bucketUpdateCollection.isEmpty()) {
            attributeUpdates.add(this.indexer.generateUpdateLastIndexedOffset(this.aggregator.getLastIndexedOffset(), lastIndexedOffset));
        }

        // Process each Key in the given Map.
        // Locate the Key's Bucket, then generate necessary Attribute Updates to integrate new Keys into it.
        for (BucketUpdate bucketUpdate : bucketUpdateCollection) {
            generateAttributeUpdates(bucketUpdate, attributeUpdates);
        }

        return attributeUpdates;
    }

    /**
     * Generates the necessary {@link AttributeUpdate}s to index updates to the given {@link BucketUpdate}.
     *
     * Backpointer updates:
     * - Backpointers are used to resolve collisions when we have exhausted the full hash (so we cannot grow the tree
     * anymore). As such, we only calculate backpointers after we group the keys based on their full hashes.
     * - We need to handle overwritten Keys, as well as linking the new Keys.
     * - When an existing Key is overwritten, the Key after it needs to be linked to the Key before it (and its link to
     * the Key before it removed). No other links between existing Keys need to be changed.
     * - New Keys need to be linked between each other, and the first one linked to the last of existing Keys.
     *
     * TableBucket updates:
     * - We need to generate sub-buckets to resolve collisions (as much as we can grow the tree). In each such sub-bucket,
     * we simply need to have the {@link TableBucket} point to the last key in updatedKeys. Backpointers will complete
     * the data structure by providing collision resolution for those remaining cases.
     *
     * @param bucketUpdate     The {@link BucketUpdate} to generate updates for.
     * @param attributeUpdates A List of {@link AttributeUpdate}s to collect into.
     */
    private void generateAttributeUpdates(BucketUpdate bucketUpdate, List<AttributeUpdate> attributeUpdates) {
        if (bucketUpdate.updatedKeys.isEmpty()) {
            // Nothing to do.
            return;
        }

        // Group all Keys by Key Hash. This will help us define the new buckets.
        val bucketsByHash = groupByKeyHash(bucketUpdate);

        // Keep track of the new sub-bucket offsets.
        HashMap<KeyHash, Long> newBucketOffsets = new HashMap<>();

        // Keep track whether the bucket was deleted (if we get at least one update, it was not).
        boolean isDeleted = true;
        for (val entry : bucketsByHash.entrySet()) {
            // All Keys in this bucket have the same full hash. If there is more than one key per such hash, then we have
            // a collision, which must be resolved.
            BucketUpdate bucket = entry.getValue();
            generateBackpointerUpdates(bucket, attributeUpdates);

            val bucketOffset = bucket.getBucketOffset();
            if (bucketOffset >= 0) {
                // We have an update.
                newBucketOffsets.put(entry.getKey(), bucketOffset);
                isDeleted = false;
            }
        }

        // 2. Bucket updates.
        if (isDeleted) {
            this.indexer.generateBucketDelete(bucketUpdate.bucket, attributeUpdates);
        } else {
            this.indexer.generateBucketUpdate(bucketUpdate.bucket, newBucketOffsets, attributeUpdates);
        }
    }

    /**
     * Groups all the keys (existing and updates) in the given {@link BucketUpdate} by their full {@link KeyHash}.
     *
     * @param bucketUpdate The BucketUpdate to group keys from. This object will not be altered.
     * @return A HashMap of {@link KeyHash} to {@link BucketUpdate}. Each pair groups Keys (Existing and Updates) by their
     * full KeyHashes.
     */
    private HashMap<KeyHash, BucketUpdate> groupByKeyHash(BucketUpdate bucketUpdate) {
        val bucketsByHash = new HashMap<KeyHash, BucketUpdate>();

        for (val e : bucketUpdate.existingKeys.values()) {
            val hash = this.hasher.hash(e.getKey().getArray());
            val bi = bucketsByHash.computeIfAbsent(hash, ignored -> new BucketUpdate(bucketUpdate.bucket));
            bi.existingKeys.put(e.getKey(), e);
        }

        for (val e : bucketUpdate.updatedKeys.values()) {
            val hash = this.hasher.hash(e.getKey().getArray());
            val bi = bucketsByHash.computeIfAbsent(hash, ignored -> new BucketUpdate(bucketUpdate.bucket));
            bi.updatedKeys.put(e.getKey(), e);
        }
        return bucketsByHash;
    }

    /**
     * Generates the necessary Backpointer updates for the given {@link BucketUpdate}.
     *
     * @param update           The BucketUpdate to generate Backpointers for.
     * @param attributeUpdates A List of {@link AttributeUpdate} where the updates will be collected.
     */
    private void generateBackpointerUpdates(BucketUpdate update, List<AttributeUpdate> attributeUpdates) {
        // Keep track of the previous, non-deleted Key's offset. The first one points to nothing.
        AtomicLong previousOffset = new AtomicLong(Attributes.NULL_ATTRIBUTE_VALUE);

        // Keep track of whether the previous Key has been replaced.
        AtomicBoolean previousReplaced = new AtomicBoolean(false);

        // Process all existing Keys, in order of Offsets, and either unlink them (if replaced) or update pointers as needed.
        update.existingKeys.entrySet().stream()
                .sorted(Comparator.comparingLong(e -> e.getValue().getOffset()))
                .forEach(e -> {
                    boolean replaced = update.updatedKeys.containsKey(e.getKey());
                    if (replaced) {
                        // This one has been replaced or removed; delete any backpointer originating from it.
                        attributeUpdates.add(this.indexer.generateBackpointerRemoval(e.getValue().getOffset()));
                        previousReplaced.set(true);
                    } else if (previousReplaced.get()) {
                        // This one hasn't been replaced or removed, however its previous one has been.
                        // Repoint it to whatever key is now ahead of it, or remove it (if previousOffset is nothing).
                        attributeUpdates.add(this.indexer.generateBackpointerUpdate(e.getValue().getOffset(), previousOffset.get()));
                        previousReplaced.set(false);
                        previousOffset.set(e.getValue().getOffset());
                    }
                });

        // Process all the new Keys, in order of offsets, and add any backpointers as needed, making sure to also link them
        // to whatever surviving existing Keys we might still have.
        update.updatedKeys.entrySet().stream()
                .filter(e -> !e.getValue().isDeleted())
                .sorted(Comparator.comparingLong(e -> e.getValue().getOffset()))
                .forEach(e -> {
                    if (previousOffset.get() != Attributes.NULL_ATTRIBUTE_VALUE) {
                        // Only add a backpointer if we have another Key ahead of it.
                        attributeUpdates.add(this.indexer.generateBackpointerUpdate(e.getValue().getOffset(), previousOffset.get()));
                    }

                    previousOffset.set(e.getValue().getOffset());
                });
    }

    //endregion

    //region OperationAggregator

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

    //endregion

    //region Helper Classes

    /**
     * Represents an update to a particular Key.
     */
    @Data
    private static class KeyUpdate {
        /**
         * The updated key.
         */
        private final HashedArray key;

        /**
         * The offset at which the update is recorded in the Segment.
         */
        private final long offset;

        /**
         * If true, indicates the Key has been deleted (as opposed from being updated).
         */
        private final boolean deleted;

        @Override
        public String toString() {
            return String.format("%sOffset=%s, Key={%s}", this.deleted ? "[DELETED] " : "", this.offset, key);
        }
    }

    /**
     * Collection of Keys to their superseding {@link KeyUpdate}s.
     */
    private static class KeyUpdateCollection extends HashMap<HashedArray, KeyUpdate> {
        /**
         * The Segment offset before which every single byte has been indexed (i.e., the last offset of the last update).
         */
        @Getter
        private long lastIndexedOffset = 0L;

        void setLastIndexedOffset(long value) {
            Preconditions.checkState(value > this.lastIndexedOffset, "lastIndexedOffset must increase");
            this.lastIndexedOffset = value;
        }
    }

    /**
     * Represents an update to a Table Bucket.
     */
    @RequiredArgsConstructor
    private static class BucketUpdate {
        /**
         * The updated bucket, as fetched by the {@link Indexer}.
         */
        private final TableBucket bucket;

        /**
         * A {@link KeyUpdateCollection} representing existing Keys and their offsets.
         */
        private final KeyUpdateCollection existingKeys = new KeyUpdateCollection();

        /**
         * A {@link KeyUpdateCollection} representing updated (or deleted) Keys and their update offsets.
         */
        private final KeyUpdateCollection updatedKeys = new KeyUpdateCollection();

        /**
         * Gets a value representing the last offset of any update - which will now become the Bucket offset.
         *
         * @return The bucket offset, or -1 if no such offset (i.e., if everything in this bucket was deleted).
         */
        long getBucketOffset() {
            return this.updatedKeys.values().stream()
                    .filter(u -> !u.isDeleted())
                    .mapToLong(KeyUpdate::getOffset).max().orElse(-1);
        }
    }


    @FunctionalInterface
    public interface GetSegment {
        CompletableFuture<DirectSegmentAccess> apply(String segmentName, Duration timeout);
    }

    //endregion
}
