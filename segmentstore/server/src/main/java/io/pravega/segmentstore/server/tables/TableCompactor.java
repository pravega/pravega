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
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

@RequiredArgsConstructor
@Slf4j
class TableCompactor {
    /**
     * The maximum size of all the entries to process at once. If needing to move, these will all be processed as a single
     * StreamSegmentAppend, so we should not make this too large.
     */
    private static final int MAX_READ_LENGTH = 4 * EntrySerializer.MAX_SERIALIZATION_LENGTH;

    @NonNull
    private final TableWriterConnector connector;
    @NonNull
    private final IndexReader indexReader;
    @NonNull
    private final Executor executor;

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
        long utilization = totalEntryCount == 0 ? 0 : MathHelpers.minMax(Math.round(100.0 * entryCount / totalEntryCount), 0, 100);
        long utilizationThreshold = (int) MathHelpers.minMax(this.indexReader.getCompactionUtilizationThreshold(info), 0, 100);
        return utilization < utilizationThreshold;
    }

    /**
     * Performs a compaction of a Table Segment. Refer to this class' Javadoc for a description of the compaction process.
     *
     * @param segment A {@link DirectSegmentAccess} providing access to the Table Segment to compact.
     * @param timer   Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain a {@link CompactionResult} summarizing the changes
     * performed. When this future completes, the some of the Segment's Table Attributes may change to reflect the
     * modifications to the Segment and/or compaction progress.
     */
    CompletableFuture<CompactionResult> compact(@NonNull DirectSegmentAccess segment, TimeoutTimer timer) {
        SegmentProperties info = segment.getInfo();
        long startOffset = getCompactionStartOffset(info);
        int maxLength = (int) MathHelpers.minMax(0, MAX_READ_LENGTH, this.indexReader.getLastIndexedOffset(info) - startOffset);
        if (startOffset < 0 || maxLength < 0) {
            return Futures.failedFuture(new DataCorruptionException(String.format(
                    "Segment[%s] (%s) has CompactionStartOffset=%s and CompactionLength=%s.", segment.getSegmentId(), info.getName(),
                    startOffset, maxLength)));
        } else if (maxLength == 0) {
            // Nothing to do.
            return CompletableFuture.completedFuture(new CompactionResult(0, startOffset));
        }

        // Read the Table Entries beginning at the specified offset, without exceeding the given maximum length.
        return readCandidates(segment, startOffset, maxLength, timer)
                .thenComposeAsync(candidates -> this.indexReader
                                .locateBuckets(segment, candidates.candidates.keySet(), timer)
                                .thenComposeAsync(buckets -> excludeObsolete(segment, candidates, buckets, timer), this.executor)
                                .thenComposeAsync(v -> moveCandidates(segment, candidates, timer), this.executor),
                        this.executor);
    }

    /**
     * Reads a set of compaction candidates from the Segment and generates a {@link Args} with them grouped
     * by their Key Hash.
     *
     * @param segment     The Segment to read from.
     * @param startOffset The offset to start reading from.
     * @param maxLength   The maximum number of bytes to read. The actual number of bytes read will be at most this, since
     *                    we can only read whole Table Entries.
     * @param timer       Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain a {@link Args} with the result.
     */
    private CompletableFuture<Args> readCandidates(DirectSegmentAccess segment, long startOffset, int maxLength, TimeoutTimer timer) {
        ReadResult rr = segment.read(startOffset, maxLength, timer.getRemaining());
        return AsyncReadResultProcessor.processAll(rr, this.executor, timer.getRemaining())
                .thenApply(inputStream -> parseEntries(inputStream, startOffset));
    }

    // TODO: continue writing Javadoc from here; re-examine logic and make sure segment offsets and versions are not confused.
    @SneakyThrows(IOException.class)
    private Args parseEntries(InputStream input, long startOffset) {
        val entries = new HashMap<UUID, CandidateSet>();
        int count = 0;
        long nextOffset = startOffset;
        try {
            while (true) {
                val e = AsyncTableEntryReader.readEntryComponents(input, nextOffset, this.connector.getSerializer());
                if (!e.getHeader().isDeletion()) {
                    // We only care about updates, and not removals.
                    val hash = this.connector.getKeyHasher().hash(e.getKey());
                    CandidateSet candidateList = entries.computeIfAbsent(hash, h -> new CandidateSet());
                    candidateList.add(new Candidate(nextOffset,
                            TableEntry.versioned(new ByteArraySegment(e.getKey()), new ByteArraySegment(e.getValue()), e.getVersion())));
                }

                // Every entry, even if a deletion, must be counted, as we will need to adjust the Segment's TOTAL_ENTRY_COUNT
                // attribute at the end.
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

        return new Args(startOffset, nextOffset, count, entries);
    }

    private CompletableFuture<Void> excludeObsolete(DirectSegmentAccess segment, Args candidates,
                                                    Map<UUID, TableBucket> buckets, TimeoutTimer timer) {
        // Exclude all those Table Entries whose buckets altogether do not exist.
        val toDelete = new ArrayList<UUID>();
        for (val hash : candidates.candidates.keySet()) {
            val bucket = buckets.get(hash);
            if (bucket == null || !bucket.exists()) {
                toDelete.add(hash);
            }
        }

        toDelete.forEach(candidates.candidates::remove);

        // For every Bucket that still exists, find all its Keys and match with our candidates.
        // - If a candidate exists with higher version/offset, exclude it.
        // - If a candidate in the index at the current offset, keep it, but remember any backpointers pointing at/from it.
        val br = TableBucketReader.key(segment, this.indexReader::getBackpointerOffset, this.executor);
        return Futures.loop(
                candidates.candidates.entrySet(),
                e -> {
                    long bucketOffset = buckets.get(e.getKey()).getSegmentOffset();
                    BiConsumer<TableKey, Long> handler = (key, offset) -> e.getValue().removeAnyWithLowerOffset(key, offset);
                    return br.findAll(bucketOffset, handler, timer).thenApply(v -> true);
                },
                this.executor);
    }

    private CompletableFuture<CompactionResult> moveCandidates(DirectSegmentAccess segment, Args candidates, TimeoutTimer timer) {
        // Collect all the candidates for copying and order them by their version.
        val toWrite = new ArrayList<TableEntry>();
        int totalLength = 0;
        for (val list : candidates.candidates.values()) {
            for (val c : list.getAll()) {
                toWrite.add(c.entry);
                totalLength += this.connector.getSerializer().getUpdateLength(c.entry);
            }
        }

        CompactionResult result = new CompactionResult(toWrite.size(), candidates.endOffset);
        val attributes = generateAttributeUpdates(candidates);
        if (totalLength == 0) {
            // Nothing to do; update the necessary segment attributes.
            return segment.updateAttributes(attributes, timer.getRemaining()).thenApply(ignored -> result);
        }

        // Perform a Segment Append with re-serialized entries (Explicit versions), and atomically update the necessary
        // segment attributes.
        byte[] appendData = new byte[totalLength];
        toWrite.sort(Comparator.comparingLong(c -> c.getKey().getVersion()));
        this.connector.getSerializer().serializeUpdateWithExplicitVersion(toWrite, appendData);
        return segment.append(appendData, attributes, timer.getRemaining()).thenApply(ignored -> result);
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
    private Collection<AttributeUpdate> generateAttributeUpdates(Args candidates) {
        return Arrays.asList(
                new AttributeUpdate(TableAttributes.COMPACTION_OFFSET, AttributeUpdateType.ReplaceIfEquals, candidates.endOffset, candidates.startOffset),
                new AttributeUpdate(TableAttributes.TOTAL_ENTRY_COUNT, AttributeUpdateType.Accumulate, -candidates.count));
    }

    private long getCompactionStartOffset(SegmentProperties info) {
        return Math.max(this.indexReader.getCompactionOffset(info), info.getStartOffset());
    }

    @RequiredArgsConstructor
    private static class Args {
        final long startOffset;
        final long endOffset;
        final int count;
        final Map<UUID, CandidateSet> candidates;
    }

    private static class CandidateSet {
        final Map<HashedArray, Candidate> byKey = new HashMap<>();

        void add(Candidate c) {
            val key = new HashedArray(c.entry.getKey().getKey());
            val existing = this.byKey.get(key);
            if (existing == null || existing.entry.getKey().getVersion() < c.entry.getKey().getVersion()) {
                // Either first time seeing this key or we saw it before with a smaller version - use this one.
                this.byKey.put(key, c);
            }
        }

        void removeAnyWithLowerOffset(TableKey existingKey, long existingKeyOffset) {
            val key = new HashedArray(existingKey.getKey());
            val c = this.byKey.get(key);
            if (c != null && c.segmentOffset < existingKeyOffset) {
                this.byKey.remove(key);
            }
        }

        Collection<Candidate> getAll() {
            return this.byKey.values();
        }
    }

    @RequiredArgsConstructor
    private static class Candidate {
        final long segmentOffset;
        final TableEntry entry;
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Getter
    static class CompactionResult {
        /**
         * Number of {@link TableEntry} instances moved.
         */
        private final int movedEntryCount;
        /**
         * Last Offset in the Table Segment that was processed as part of this compaction. The next compaction iteration
         * should start at this offset.
         */
        private final long lastProcessedOffset;

        @Override
        public String toString() {
            return String.format("Count = %d, LastOffset = %d", this.movedEntryCount, this.lastProcessedOffset);
        }
    }
}
