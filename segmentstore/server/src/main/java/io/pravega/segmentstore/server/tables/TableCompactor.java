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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.common.MathHelpers;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * TODO: Javadoc
 * TODO: logging
 */
@Slf4j
class TableCompactor {
    //region Members

    /**
     * Default value for {@link #maxReadLength}, if not supplied via constructor.
     */
    static final int DEFAULT_MAX_READ_LENGTH = 4 * EntrySerializer.MAX_SERIALIZATION_LENGTH;

    @NonNull
    private final TableWriterConnector connector;
    @NonNull
    private final IndexReader indexReader;
    @NonNull
    private final Executor executor;
    /**
     * The maximum size of all the entries to process at once. If needing to move, these will all be processed as a single
     * StreamSegmentAppend, so we should not make this too large.
     */
    private final int maxReadLength;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the {@link TableCompactor} class.
     *
     * @param connector   The {@link TableWriterConnector} to use to interface with the Table Segments to compact.
     * @param indexReader A {@link IndexReader} that provides a read-only view of a Table Segment's index.
     * @param executor    Executor for async operations.
     */
    TableCompactor(TableWriterConnector connector, IndexReader indexReader, Executor executor) {
        this(connector, indexReader, executor, DEFAULT_MAX_READ_LENGTH);
    }

    /**
     * Creates a new instance of the {@link TableCompactor} class.
     *
     * @param connector     The {@link TableWriterConnector} to use to interface with the Table Segments to compact.
     * @param indexReader   A {@link IndexReader} that provides a read-only view of a Table Segment's index.
     * @param executor      Executor for async operations.
     * @param maxReadLength Maximum number of bytes to read for every compaction. This number must be sufficiently large
     *                      to be able to read at least one Table Entry.
     */
    @VisibleForTesting
    TableCompactor(@NonNull TableWriterConnector connector, @NonNull IndexReader indexReader, @NonNull Executor executor, int maxReadLength) {
        Preconditions.checkArgument(maxReadLength > 0, "maxReadLength must be a positive number.");
        this.connector = connector;
        this.indexReader = indexReader;
        this.executor = executor;
        this.maxReadLength = maxReadLength;
    }

    //endregion

    /**
     * Determines if Table Compaction is required on a Table Segment.
     *
     * @param info The {@link SegmentProperties} associated with the Table Segment to inquire about.
     * @return True if compaction is required, false otherwise.
     */
    boolean isCompactionRequired(SegmentProperties info) {
        long startOffset = getCompactionStartOffset(info);
        long lastIndexOffset = this.indexReader.getLastIndexedOffset(info);
        if (startOffset >= lastIndexOffset) {
            // Either nothing was indexed or compaction has already reached the indexed limit.
            return false;
        }

        long totalEntryCount = this.indexReader.getTotalEntryCount(info);
        long entryCount = this.indexReader.getEntryCount(info);
        long utilization = totalEntryCount == 0 ? 100 : MathHelpers.minMax(Math.round(100.0 * entryCount / totalEntryCount), 0, 100);
        long utilizationThreshold = (int) MathHelpers.minMax(this.indexReader.getCompactionUtilizationThreshold(info), 0, 100);
        return utilization < utilizationThreshold;
    }

    /**
     * Calculates the offset in the Segment where it is safe to truncate based on the current state of the Segment and
     * the highest copied offset encountered during an index update.
     *
     * @param info                The {@link SegmentProperties} associated with the Table Segment to inquire about.
     * @param highestCopiedOffset The highest offset that was copied from a lower offset during a compaction. If the copied
     *                            entry has already been index then it is guaranteed that every entry prior to this
     *                            offset is no longer part of the index and can be safely truncated away.
     * @return The calculated truncation offset or a negative number if no truncation is required or possible given the
     * arguments we got.
     */
    long calculateTruncationOffset(SegmentProperties info, long highestCopiedOffset) {
        // Due to the nature of compaction (all entries are copied in order of their original versions), if we encounter
        // any copied Table Entries then the highest explicit version defined on any of them is
        if (highestCopiedOffset > 0) {
            return highestCopiedOffset;
        }
        // Did not encounter any copied entries. If we were able to index the whole segment, then we should be safe
        // to truncate at wherever the compaction last finished.
        long truncateOffset = -1;
        if (this.indexReader.getLastIndexedOffset(info) >= info.getLength()) {
            truncateOffset = this.indexReader.getCompactionOffset(info);
        }

        if (truncateOffset <= info.getStartOffset()) {
            // The segment is already truncated at the compaction offset; no need for more.
            truncateOffset = -1;
        }

        return truncateOffset;
    }

    /**
     * Performs a compaction of a Table Segment. Refer to this class' Javadoc for a description of the compaction process.
     *
     * @param segment A {@link DirectSegmentAccess} providing access to the Table Segment to compact.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, indicate the compaction completed. When this future completes,
     * some of the Segment's Table Attributes may change to reflect the modifications to the Segment and/or compaction progress.
     */
    CompletableFuture<Void> compact(@NonNull DirectSegmentAccess segment, TimeoutTimer timer) {
        // TODO: retry in case of BadAttributeUpdateException
        SegmentProperties info = segment.getInfo();
        long startOffset = getCompactionStartOffset(info);
        int maxLength = (int) Math.min(this.maxReadLength, this.indexReader.getLastIndexedOffset(info) - startOffset);
        if (startOffset < 0 || maxLength < 0) {
            // The Segment's Compaction offset must be a value between 0 and the current LastIndexedOffset.
            return Futures.failedFuture(new DataCorruptionException(String.format(
                    "Segment[%s] (%s) has CompactionStartOffset=%s and CompactionLength=%s.",
                    segment.getSegmentId(), info.getName(), startOffset, maxLength)));
        } else if (maxLength == 0) {
            // Nothing to do.
            return CompletableFuture.completedFuture(null);
        }

        // Read the Table Entries beginning at the specified offset, without exceeding the given maximum length.
        return readCandidates(segment, startOffset, maxLength, timer)
                .thenComposeAsync(candidates -> this.indexReader
                                .locateBuckets(segment, candidates.candidates.keySet(), timer)
                                .thenComposeAsync(buckets -> excludeObsolete(segment, candidates, buckets, timer), this.executor)
                                .thenComposeAsync(v -> copyCandidates(segment, candidates, timer), this.executor),
                        this.executor);
    }

    /**
     * Reads a set of compaction candidates from the Segment and generates a {@link CompactionArgs} with them grouped
     * by their Key Hash.
     *
     * @param segment     The Segment to read from.
     * @param startOffset The offset to start reading from.
     * @param maxLength   The maximum number of bytes to read. The actual number of bytes read will be at most this, since
     *                    we can only read whole Table Entries.
     * @param timer       Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain a {@link CompactionArgs} with the result.
     */
    private CompletableFuture<CompactionArgs> readCandidates(DirectSegmentAccess segment, long startOffset, int maxLength, TimeoutTimer timer) {
        ReadResult rr = segment.read(startOffset, maxLength, timer.getRemaining());
        return AsyncReadResultProcessor.processAll(rr, this.executor, timer.getRemaining())
                .thenApply(inputStream -> parseEntries(inputStream, startOffset));
    }

    /**
     * Parses out a {@link CompactionArgs} object containing Compaction {@link Candidate}s from the given InputStream
     * representing Segment data.
     *
     * @param input       An InputStream representing a continuous range of bytes in the Segment.
     * @param startOffset The offset at which the InputStream begins. This should be the Compaction Offset.
     * @return A {@link CompactionArgs} object containing the result.
     */
    @SneakyThrows(IOException.class)
    private CompactionArgs parseEntries(InputStream input, long startOffset) {
        val entries = new HashMap<UUID, CandidateSet>();
        int count = 0;
        long nextOffset = startOffset;
        try {
            while (true) {
                // TODO: Handle data corruption event when compaction offset is not on Entry boundary. Need github issue.
                val e = AsyncTableEntryReader.readEntryComponents(input, nextOffset, this.connector.getSerializer());

                // We only care about updates, and not removals.
                if (!e.getHeader().isDeletion()) {
                    // Group by KeyHash, and deduplicate (based on key).
                    val hash = this.connector.getKeyHasher().hash(e.getKey());
                    CandidateSet candidates = entries.computeIfAbsent(hash, h -> new CandidateSet());
                    candidates.add(new Candidate(nextOffset,
                            TableEntry.versioned(new ByteArraySegment(e.getKey()), new ByteArraySegment(e.getValue()), e.getVersion())));
                }

                // Every entry, even if deleted or duplicated, must be counted, as we will need to adjust the Segment's
                // TOTAL_ENTRY_COUNT attribute at the end.
                count++;

                // Update the offset to the beginning of the next entry.
                nextOffset += e.getHeader().getTotalLength();
            }
        } catch (EOFException ex) {
            // We chose an arbitrary read length, so it is quite possible we stopped reading in the middle of an entry.
            // As such, EOFException is the only way to know when to stop. When this happens, we will have collected the
            // total read length in segmentOffset.
            input.close();
        }

        return new CompactionArgs(startOffset, nextOffset, count, entries);
    }

    /**
     * Processes the given {@link CompactionArgs} and eliminates all {@link Candidate}s that meet at least one of the
     * following criteria:
     * - The Key's Table Bucket is no longer part of the index (removal)
     * - The Key exists in the Index, but the Index points to a newer version of it.
     *
     * @param segment A {@link DirectSegmentAccess} representing the Segment to operate on.
     * @param args    A {@link CompactionArgs} representing the set of {@link Candidate}s for compaction. This set
     *                will be modified based on the outcome of this method.
     * @param buckets The Buckets retrieved via the {@link IndexReader} for the {@link Candidate}s.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will indicate the operation has finished.
     */
    private CompletableFuture<Void> excludeObsolete(DirectSegmentAccess segment, CompactionArgs args,
                                                    Map<UUID, TableBucket> buckets, TimeoutTimer timer) {
        // Exclude all those Table Entries whose buckets altogether do not exist.
        args.candidates.keySet().removeIf(hash -> {
            val bucket = buckets.get(hash);
            return bucket == null || !bucket.exists();
        });

        // For every Bucket that still exists, find all its Keys and match with our candidates and figure out if our
        // candidates are still eligible for compaction.
        val br = TableBucketReader.key(segment, this.indexReader::getBackpointerOffset, this.executor);
        val candidates = args.candidates.entrySet().iterator();
        return Futures.loop(
                candidates::hasNext,
                () -> {
                    val e = candidates.next();
                    long bucketOffset = buckets.get(e.getKey()).getSegmentOffset();
                    BiConsumer<TableKey, Long> handler = (key, offset) -> e.getValue().handleExistingKey(key, offset);
                    return br.findAll(bucketOffset, handler, timer);
                },
                this.executor);
    }

    /**
     * Copies the {@link Candidate}s in the given {@link CompactionArgs} set to a contiguous block at the end of the Segment.
     *
     * @param segment A {@link DirectSegmentAccess} representing the Segment to operate on.
     * @param args    A {@link CompactionArgs} containing the {@link Candidate}s to copy.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, indicate the candidates have been copied.
     */
    private CompletableFuture<Void> copyCandidates(DirectSegmentAccess segment, CompactionArgs args, TimeoutTimer timer) {
        // Collect all the candidates for copying and calculate the total serialization length.
        val toWrite = new ArrayList<TableEntry>();
        int totalLength = 0;
        for (val list : args.candidates.values()) {
            for (val c : list.getAll()) {
                toWrite.add(c.entry);
                totalLength += this.connector.getSerializer().getUpdateLength(c.entry);
            }
        }

        // Generate the necessary AttributeUpdates that will need to be applied regardless of whether we copy anything or not.
        val attributes = generateAttributeUpdates(args);
        CompletableFuture<?> result;
        if (totalLength == 0) {
            // Nothing to do; update the necessary segment attributes.
            assert toWrite.size() == 0;
            result = segment.updateAttributes(attributes, timer.getRemaining());
        } else {
            // Perform a Segment Append with re-serialized entries (Explicit versions), and atomically update the necessary
            // segment attributes.
            toWrite.sort(Comparator.comparingLong(c -> c.getKey().getVersion()));
            byte[] appendData = new byte[totalLength];
            this.connector.getSerializer().serializeUpdateWithExplicitVersion(toWrite, appendData);
            result = segment.append(appendData, attributes, timer.getRemaining());
        }

        return Futures.toVoid(result);
    }

    /**
     * Generates a Collection of {@link AttributeUpdate}s that will be applied to the Segment at the end of each compaction.
     * The following {@link TableAttributes} are modified:
     * - {@link TableAttributes#COMPACTION_OFFSET}: update to where we finished, so that the next compaction can resume
     * from there.
     * - {@link TableAttributes#TOTAL_ENTRY_COUNT}: reduce by the total number of candidates we encountered (including
     * obsoletes, deletions and those that will be moved); the {@link IndexWriter} will update this back when reindexing
     * the moved Table Entries.
     * - {@link TableAttributes#ENTRY_COUNT}: this is not modified - it keeps track of how many Table Entries are active
     * (there's an index entry for them); the IndexWriter will update it as appropriate based on the state of the Index
     * at the time the moved Table Entries will be reindexed.
     *
     * @param candidates The Candidates for compaction.
     * @return The result.
     */
    private Collection<AttributeUpdate> generateAttributeUpdates(CompactionArgs candidates) {
        return Arrays.asList(
                new AttributeUpdate(TableAttributes.COMPACTION_OFFSET, AttributeUpdateType.ReplaceIfEquals, candidates.endOffset, candidates.startOffset),
                new AttributeUpdate(TableAttributes.TOTAL_ENTRY_COUNT, AttributeUpdateType.Accumulate, -candidates.count));
    }

    /**
     * Calculates the Segment Offset where to start the compaction at. This is the maximum of the Segment's
     * {@link TableAttributes#COMPACTION_OFFSET} attribute and the Segment's StartOffset (where it is truncated).
     *
     * @param info A {@link SegmentProperties} representing the current state of the Segment.
     * @return The Segment Offset where to begin compaction at.
     */
    private long getCompactionStartOffset(SegmentProperties info) {
        return Math.max(this.indexReader.getCompactionOffset(info), info.getStartOffset());
    }

    //region Helper Classes

    @RequiredArgsConstructor
    private static class CompactionArgs {
        /**
         * Offset where compaction began from.
         */
        final long startOffset;

        /**
         * Last offset that was processed for compaction. This will lie at a TableEntry's boundary and indicates the
         * location where the next compaction can begin from.
         */
        final long endOffset;

        /**
         * Number of Table Entries processed, including duplicates, obsoletes and removals.
         */
        final int count;

        /**
         * Candidates for compaction, grouped by their Key Hash.
         */
        final Map<UUID, CandidateSet> candidates;
    }

    private static class CandidateSet {
        /**
         * Candidates, indexed by their TableKey.
         */
        final Map<HashedArray, Candidate> byKey = new HashMap<>();

        /**
         * Adds the given {@link Candidate}, but only if its Key does not exist or exists with a lower version.
         *
         * @param c The {@link Candidate} to add.
         */
        void add(Candidate c) {
            val key = new HashedArray(c.entry.getKey().getKey());
            val existing = this.byKey.get(key);
            if (existing == null || existing.entry.getKey().getVersion() < c.entry.getKey().getVersion()) {
                // Either first time seeing this key or we saw it before with a smaller version - use this one.
                this.byKey.put(key, c);
            }
        }

        /**
         * Excludes any {@link Candidate}s from the {@link CandidateSet} that match the given key but have
         *
         * @param existingKey       A {@link TableKey} that exists and is included in the index.
         * @param existingKeyOffset The existing Key's offset.
         */
        void handleExistingKey(TableKey existingKey, long existingKeyOffset) {
            val key = new HashedArray(existingKey.getKey());
            val c = this.byKey.get(key);
            if (c != null && c.segmentOffset < existingKeyOffset) {
                this.byKey.remove(key);
            }
        }

        /**
         * Gets an unordered collection of {@link Candidate}s in this set.
         *
         * @return The result.
         */
        Collection<Candidate> getAll() {
            return this.byKey.values();
        }
    }

    @RequiredArgsConstructor
    private static class Candidate {
        /**
         * The offset at which this candidate resides.
         */
        final long segmentOffset;

        /**
         * The Table Entry that is a candidate for copying..
         */
        final TableEntry entry;

        @Override
        public String toString() {
            return String.format("Offset = %d, Entry = {%s}", this.segmentOffset, this.entry);
        }
    }

    //endregion
}
