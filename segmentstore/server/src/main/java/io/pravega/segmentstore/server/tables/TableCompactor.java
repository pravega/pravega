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
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
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
    private final DirectSegmentAccess segment;
    @NonNull
    private final KeyHasher hasher;
    @NonNull
    private final EntrySerializer serializer;
    @NonNull
    private final IndexWriter indexWriter;
    @NonNull
    private final Executor executor;
    @NonNull
    private final String traceObjectId;

    static CompletableFuture<Void> compactIfNeeded(DirectSegmentAccess segment, TableWriterConnector connector,
                                                   IndexWriter indexWriter, Executor executor, Duration timeout) {
        // Decide if compaction is needed. If not, bail out early.
        SegmentState state = new SegmentState(segment.getInfo());
        long startOffset = Math.max(state.compactionOffset, state.startOffset);
        int maxLength = (int) Math.min(MAX_READ_LENGTH, state.lastIndexOffset - startOffset);
        if (state.utilization >= state.utilizationThreshold || maxLength <= 0) {
            // TODO: log.debug().
            return CompletableFuture.completedFuture(null);
        }

        String traceObjectId = String.format("TableCompactor[%d-%d]", connector.getMetadata().getContainerId(), connector.getMetadata().getId());
        TableCompactor c = new TableCompactor(segment, connector.getKeyHasher(), connector.getSerializer(), indexWriter, executor, traceObjectId);
        return c.compactOnce(startOffset, maxLength, timeout);
    }

    @VisibleForTesting
    CompletableFuture<Void> compactOnce(long startOffset, int maxLength, Duration timeout) {
        Preconditions.checkArgument(startOffset >= 0, "startOffset must be a non-negative number.");
        Preconditions.checkArgument(maxLength > 0, "maxLength must be a positive number.");

        // Read the Table Entries beginning at the specified offset, without exceeding the given maximum length.
        TimeoutTimer timer = new TimeoutTimer(timeout);
        return readEntriesFromSegment(this.segment, startOffset, maxLength, timer)
                .thenComposeAsync(candidates -> excludeObsolete(candidates, timer), this.executor)
                .thenComposeAsync(candidates -> moveCandidates(candidates, timer), this.executor);
    }

    private CompletableFuture<CandidateCollection> readEntriesFromSegment(DirectSegmentAccess segment, long startOffset, int maxLength, TimeoutTimer timer) {
        ReadResult rr = segment.read(startOffset, maxLength, timer.getRemaining());
        return AsyncReadResultProcessor.processAll(rr, this.executor, timer.getRemaining())
                .thenApply(inputStream -> parseEntries(inputStream, startOffset));
    }

    @SneakyThrows(IOException.class)
    private CandidateCollection parseEntries(InputStream input, long startOffset) {
        val entries = new HashMap<UUID, List<Candidate>>();
        long nextOffset = startOffset;
        try {
            while (true) {
                val e = AsyncTableEntryReader.readEntryComponents(input, nextOffset, this.serializer);
                if (!e.getHeader().isDeletion()) {
                    // We only care about updates, and not removals.
                    val hash = this.hasher.hash(e.getKey());
                    List<Candidate> candidateList = entries.computeIfAbsent(hash, h -> new ArrayList<>());
                    candidateList.add(new Candidate(nextOffset,
                            TableEntry.versioned(new ByteArraySegment(e.getKey()), new ByteArraySegment(e.getValue()), e.getVersion())));
                }

                nextOffset += e.getHeader().getTotalLength();
            }
        } catch (EOFException ex) {
            // We chose an arbitrary read length, so it is quite possible we stopped reading in the middle of an entry.
            // As such, EOFException is the only way to know when to stop. When this happens, we will have collected the
            // total read length in segmentOffset.
            input.close();
        }

        return new CandidateCollection(startOffset, nextOffset, entries);
    }

    private CompletableFuture<CandidateCollection> excludeObsolete(CandidateCollection candidates, TimeoutTimer timer) {
        return this.indexWriter.locateBuckets(this.segment, candidates.candidates.keySet(), timer)
                .thenComposeAsync(buckets -> excludeObsolete(candidates, buckets, timer), this.executor);
    }

    private CompletableFuture<CandidateCollection> excludeObsolete(CandidateCollection candidates, Map<UUID, TableBucket> buckets, TimeoutTimer timer) {
        // Exclude all those Table Entries whose buckets altogether do not exist.
        val deletedBuckets = new ArrayList<UUID>();
        for (val keyHash : candidates.candidates.keySet()) {
            val bucket = buckets.get(keyHash);
            if (bucket == null || !bucket.exists()) {
                deletedBuckets.add(keyHash);
            }
        }

        deletedBuckets.forEach(candidates.candidates::remove);

        // For every Bucket that still exists, find all its Keys and match with our candidates.
        // - If a candidate exists with higher version/offset, exclude it.
        // - If a candidate in the index at the current offset, keep it, but remember any backpointers pointing at/from it.
        // TODO: fix method below and handler. See TODO file.
        val br = TableBucketReader.key(this.segment, this.indexWriter::getBackpointerOffset, this.executor);
        return Futures.loop(
                candidates.candidates.entrySet(),
                e -> br.findAll(buckets.get(e.getKey()).getSegmentOffset(), null, timer).thenApply(v -> true),
                this.executor)
                .thenApply(ignored -> candidates);
    }

    private CompletableFuture<Void> moveCandidates(CandidateCollection candidateCollection, TimeoutTimer timer) {
        // 3.1. Perform a Segment Append with re-serialized entries (Explicit versions) and atomic Index Updates.
        // 3.2. Update Table Attributes and Truncate at the latest offset
        // TODO: See TODO file.
        return null;
    }

    @RequiredArgsConstructor
    private static class CandidateCollection {
        final long startOffset;
        final long endOffset;
        final Map<UUID, List<Candidate>> candidates;
    }

    @RequiredArgsConstructor
    private static class Candidate {
        final long offset;
        final TableEntry entry;
    }

    //region SegmentState

    @RequiredArgsConstructor
    private static class SegmentState {
        final long compactionOffset;
        final long lastIndexOffset;
        final long startOffset;
        final long entryCount;
        final long totalEntryCount;
        final int utilization;
        final int utilizationThreshold;

        SegmentState(SegmentProperties segmentInfo) {
            this.startOffset = segmentInfo.getStartOffset();

            Map<UUID, Long> attributes = segmentInfo.getAttributes();
            //TODO: use methods from IndexReader?
            this.compactionOffset = attributes.getOrDefault(TableAttributes.COMPACTION_OFFSET, this.startOffset);
            this.lastIndexOffset = attributes.getOrDefault(TableAttributes.INDEX_OFFSET, 0L);
            this.entryCount = attributes.getOrDefault(TableAttributes.ENTRY_COUNT, 0L);
            this.totalEntryCount = attributes.getOrDefault(TableAttributes.TOTAL_ENTRY_COUNT, 0L);
            this.utilizationThreshold = (int) MathHelpers.minMax(attributes.getOrDefault(TableAttributes.MIN_UTILIZATION, 0L), 0, 100);
            this.utilization = this.totalEntryCount == 0
                    ? 0
                    : MathHelpers.minMax((int) Math.round(100.0 * this.entryCount / this.totalEntryCount), 0, 100);
        }

        @Override
        public String toString() {
            return String.format("Start=%d/%d, End=%d, Entries=%d/%d, Util=%d%%/%d%%", this.compactionOffset, this.startOffset,
                    this.lastIndexOffset, this.entryCount, this.totalEntryCount, this.utilization, this.utilizationThreshold);
        }
    }

    //endregion
}
