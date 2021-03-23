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
package io.pravega.segmentstore.server.tables;

import io.pravega.common.MathHelpers;
import io.pravega.common.TimeoutTimer;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.io.SerializationException;
import io.pravega.common.util.BufferView;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
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
 * Performs {@link TableEntry} compaction in Table Segments.
 *
 * A Compaction is performed along these lines:
 * - Compaction begins at the Table Segment's {@link TableAttributes#COMPACTION_OFFSET} offset.
 * - A number of {@link TableEntry} instances are parsed out until a maximum length is reached.
 * - A parsed {@link TableEntry} is discarded if any of the following are true:
 * -- It indicates a {@link TableKey} removal.
 * -- Its {@link TableKey} is no longer part of the Table Segment's index.
 * -- Its {@link TableKey} is part of the Table Segment's index, but the index points to a newer version of it.
 * - Any {@link TableEntry} instances that are not discarded are copied over (sorted by original offset) to the end
 * of the Table Segment by using {@link DirectSegmentAccess#append}. The newly written {@link TableEntry} instances will
 * be serialized using explicit versions; as such the original {@link TableKey#getVersion()} will be preserved, even if
 * the entry will now live at a higher offset. Some of the Table Segment's attributes are also atomically updated as part
 * of this operation (namely {@link TableAttributes#COMPACTION_OFFSET} and {@link TableAttributes#TOTAL_ENTRY_COUNT}).
 * - These copied entries are not indexed as part of compaction. Similarly to normal updates, the {@link WriterTableProcessor}
 * will pick them up and index them.
 */
@Slf4j
@RequiredArgsConstructor
class TableCompactor {
    //region Members

    @NonNull
    private final TableWriterConnector connector;
    @NonNull
    private final IndexReader indexReader;
    @NonNull
    private final Executor executor;

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
        if (startOffset + this.connector.getMaxCompactionSize() >= lastIndexOffset) {
            // Either:
            // 1. Nothing was indexed
            // 2. Compaction has already reached the indexed limit.
            // 3. Not enough "uncompacted" data - at least this.maxCompactLength must be accumulated to trigger a compaction.
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
     * This method is invoked from the {@link WriterTableProcessor} after indexing. Since compaction is loosely coupled
     * with indexing, the {@link WriterTableProcessor} does not have too many insights into what has been compacted or not;
     * it can only decide based on the current state of the Table Segment and what it has just indexed. As such:
     * - If recently indexed Table Entries indicate they were copied as part of a compaction, then it is safe to truncate
     * at the highest copied offset encountered (since they are copied in order, by offset). Everything prior to this
     * offset is guaranteed not to exist in the index anymore.
     * - If no recently indexed Table Entry indicates it was copied as a result of compaction, then it may not be safe to
     * truncate at {@link TableAttributes#COMPACTION_OFFSET}, because there may exist unindexed Table Entries that the
     * indexer hasn't gotten to yet. As such, it is only safe to truncate at {@link TableAttributes#COMPACTION_OFFSET}
     * if the indexer has indexed all the entries in the Table Segment.
     *
     * @param info                The {@link SegmentProperties} associated with the Table Segment to inquire about.
     * @param highestCopiedOffset The highest offset that was copied from a lower offset during a compaction. If the copied
     *                            entry has already been index then it is guaranteed that every entry prior to this
     *                            offset is no longer part of the index and can be safely truncated away.
     * @return The calculated truncation offset or a negative number if no truncation is required or possible given the
     * arguments provided.
     */
    long calculateTruncationOffset(SegmentProperties info, long highestCopiedOffset) {
        long truncateOffset = -1;
        if (highestCopiedOffset > 0) {
            // Due to the nature of compaction (all entries are copied in order of their original versions), if we encounter
            // any copied Table Entries then the highest explicit version defined on any of them is where we can truncate.
            truncateOffset = highestCopiedOffset;
        } else if (this.indexReader.getLastIndexedOffset(info) >= info.getLength()) {
            // Did not encounter any copied entries. If we were able to index the whole segment, then we should be safe
            // to truncate at wherever the compaction last finished.
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
     * Notable exceptions:
     * <ul>
     * <li>{@link BadAttributeUpdateException} If the {@link TableAttributes#COMPACTION_OFFSET} changed while this method
     * was executing. In this case, no change will be performed and it can be resolved with a retry.</li>
     * </ul>
     */
    CompletableFuture<Void> compact(@NonNull DirectSegmentAccess segment, TimeoutTimer timer) {
        SegmentProperties info = segment.getInfo();
        long startOffset = getCompactionStartOffset(info);
        int maxLength = (int) Math.min(this.connector.getMaxCompactionSize(), this.indexReader.getLastIndexedOffset(info) - startOffset);
        if (startOffset < 0 || maxLength < 0) {
            // The Segment's Compaction offset must be a value between 0 and the current LastIndexedOffset.
            return Futures.failedFuture(new DataCorruptionException(String.format(
                    "Segment[%s] (%s) has CompactionStartOffset=%s and CompactionLength=%s.",
                    segment.getSegmentId(), info.getName(), startOffset, maxLength)));
        } else if (maxLength == 0) {
            // Nothing to do.
            log.debug("TableCompactor[{}]: Up to date.", segment.getSegmentId());
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
                .thenApply(inputData -> parseEntries(inputData, startOffset, maxLength));
    }

    /**
     * Parses out a {@link CompactionArgs} object containing Compaction {@link Candidate}s from the given InputStream
     * representing Segment data.
     *
     * @param inputData   A BufferView representing a continuous range of bytes in the Segment.
     * @param startOffset The offset at which the InputStream begins. This should be the Compaction Offset.
     * @param maxLength   The maximum number of bytes read. The given InputStream should have at most this number of
     *                    bytes in it.
     * @return A {@link CompactionArgs} object containing the result.
     */
    @SneakyThrows(SerializationException.class)
    private CompactionArgs parseEntries(BufferView inputData, long startOffset, int maxLength) {
        val entries = new HashMap<UUID, CandidateSet>();
        int count = 0;
        long nextOffset = startOffset;
        final long maxOffset = startOffset + maxLength;
        val input = inputData.getBufferViewReader();
        try {
            while (nextOffset < maxOffset) {
                // TODO: Handle error when compaction offset is not on Entry boundary (https://github.com/pravega/pravega/issues/3560).
                val e = AsyncTableEntryReader.readEntryComponents(input, nextOffset, this.connector.getSerializer());

                // We only care about updates, and not removals.
                if (!e.getHeader().isDeletion()) {
                    // Group by KeyHash, and deduplicate (based on key).
                    val hash = this.connector.getKeyHasher().hash(e.getKey());
                    CandidateSet candidates = entries.computeIfAbsent(hash, h -> new CandidateSet());
                    candidates.add(new Candidate(nextOffset,
                            TableEntry.versioned(e.getKey(), e.getValue(), e.getVersion())));
                }

                // Every entry, even if deleted or duplicated, must be counted, as we will need to adjust the Segment's
                // TOTAL_ENTRY_COUNT attribute at the end.
                count++;

                // Update the offset to the beginning of the next entry.
                nextOffset += e.getHeader().getTotalLength();
            }
        } catch (BufferView.Reader.OutOfBoundsException ex) {
            // We chose an arbitrary compact length, so it is quite possible we stopped reading in the middle of an entry.
            // As such, BufferView.Reader.OutOfBoundsException is the only way to know when to stop. When this happens,
            // we will have collected the total compact length in segmentOffset.
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
            BufferView appendData = this.connector.getSerializer().serializeUpdateWithExplicitVersion(toWrite);
            result = segment.append(appendData, attributes, timer.getRemaining());
            log.debug("TableCompactor[{}]: Compacting {}, CopyCount={}, CopyLength={}.", segment.getSegmentId(), args, toWrite.size(), totalLength);
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

        @Override
        public String toString() {
            return String.format("StartOffset=%s, EndOffset=%s, ProcessedCount=%s, CandidateCount=%s",
                    this.startOffset, this.endOffset, this.count, this.candidates.size());
        }
    }

    private static class CandidateSet {
        /**
         * Candidates, indexed by their TableKey.
         */
        final Map<BufferView, Candidate> byKey = new HashMap<>();

        /**
         * Adds the given {@link Candidate}, but only if its Key does not exist or exists with a lower version.
         *
         * @param c The {@link Candidate} to add.
         */
        void add(Candidate c) {
            val key = c.entry.getKey().getKey();
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
            val key = existingKey.getKey();
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
