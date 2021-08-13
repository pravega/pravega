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
import io.pravega.common.util.Retry;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateCollection;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.tables.TableAttributes;
import io.pravega.segmentstore.contracts.tables.TableEntry;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.DataCorruptionException;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.reading.AsyncReadResultProcessor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * Base Table Segment Compactor. Performs {@link TableEntry} compaction in Table Segments.
 * <p>
 * A Compaction is performed along these lines, regardless of Table Segment Type:
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
abstract class TableCompactor {
    //region Members

    private static final EntrySerializer SERIALIZER = new EntrySerializer();
    protected final DirectSegmentAccess segment;
    protected final SegmentMetadata metadata;
    protected final Config config;
    protected final ScheduledExecutorService executor;
    protected final String traceLogId;

    //endregion

    //region Constructor

    TableCompactor(@NonNull DirectSegmentAccess segment, @NonNull Config config, @NonNull ScheduledExecutorService executor) {
        this.segment = segment;
        this.config = config;
        this.executor = executor;
        this.metadata = segment.getInfo();
        this.traceLogId = String.format("TableCompactor[%s-%s]", this.metadata.getContainerId(), this.metadata.getId());
    }

    //endregion

    //region Layout-Specific Properties.

    /**
     * Gets the offset up to which all Table Entries have been indexed in this Table Segment.
     *
     * @return The last indexed offset.
     */
    protected abstract long getLastIndexedOffset();

    /**
     * Gets the number of unique keys in the Table Segment.
     *
     * @return A CompletableFuture, that, when completed, will contain the number of unique keys.
     */
    protected abstract CompletableFuture<Long> getUniqueEntryCount();

    /**
     * Creates a new instance of the {@link CompactionArgs} (or an implementation specific sub-class).
     *
     * @param startOffset The offset at which compaction begins.
     * @return A new {@link CompactionArgs} instance.
     */
    protected CompactionArgs newCompactionArgs(long startOffset) {
        return new CompactionArgs(startOffset);
    }

    //endregion

    /**
     * Determines if Table Compaction is required on the Table Segment.
     *
     * @return A CompletableFuture that will be completed with {@link Boolean#TRUE} if compaction is required, or
     * {@link Boolean#FALSE} otherwise.
     */
    CompletableFuture<Boolean> isCompactionRequired() {
        final long startOffset = getCompactionStartOffset();
        final long lastIndexOffset = getLastIndexedOffset();
        if (startOffset + this.config.getMaxCompactionSize() >= lastIndexOffset) {
            // Either:
            // 1. Nothing was indexed
            // 2. Compaction has already reached the indexed limit.
            // 3. Not enough "uncompacted" data - at least this.connector.getMaxCompactionSize() must be accumulated to trigger a compaction.
            return CompletableFuture.completedFuture(false);
        }

        final long totalEntryCount = IndexReader.getTotalEntryCount(this.metadata);
        final long utilizationThreshold = (int) MathHelpers.minMax(IndexReader.getCompactionUtilizationThreshold(this.metadata), 0, 100);
        return getUniqueEntryCount()
                .thenApply(entryCount -> {
                    final long utilization = totalEntryCount == 0 ? 100 : MathHelpers.minMax(Math.round(100.0 * entryCount / totalEntryCount), 0, 100);
                    return utilization < utilizationThreshold;
                });
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
     * @param highestCopiedOffset The highest offset that was copied from a lower offset during a compaction. If the copied
     *                            entry has already been index then it is guaranteed that every entry prior to this
     *                            offset is no longer part of the index and can be safely truncated away.
     * @return The calculated truncation offset or a negative number if no truncation is required or possible given the
     * arguments provided.
     */
    long calculateTruncationOffset(long highestCopiedOffset) {
        long truncateOffset = -1;
        if (highestCopiedOffset > 0) {
            // Due to the nature of compaction (all entries are copied in order of their original versions), if we encounter
            // any copied Table Entries then the highest explicit version defined on any of them is where we can truncate.
            truncateOffset = highestCopiedOffset;
        } else if (getLastIndexedOffset() >= this.metadata.getLength()) {
            // Did not encounter any copied entries. If we were able to index the whole segment, then we should be safe
            // to truncate at wherever the compaction last finished.
            truncateOffset = IndexReader.getCompactionOffset(this.metadata);
        }

        if (truncateOffset <= this.metadata.getStartOffset()) {
            // The segment is already truncated at the compaction offset; no need for more.
            truncateOffset = -1;
        }

        return truncateOffset;
    }

    /**
     * Performs a compaction of the Table Segment. Refer to this class' Javadoc for a description of the compaction process.
     *
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed, indicate the compaction completed. When this future completes,
     * some of the Segment's Table Attributes may change to reflect the modifications to the Segment and/or compaction progress.
     * Notable exceptions:
     * <ul>
     * <li>{@link BadAttributeUpdateException} If the {@link TableAttributes#COMPACTION_OFFSET} changed while this method
     * was executing. In this case, no change will be performed and it can be resolved with a retry.</li>
     * </ul>
     */
    CompletableFuture<Void> compact(TimeoutTimer timer) {
        long startOffset = getCompactionStartOffset();
        int maxLength = (int) Math.min(this.config.getMaxCompactionSize(), getLastIndexedOffset() - startOffset);
        if (startOffset < 0 || maxLength < 0) {
            // The Segment's Compaction offset must be a value between 0 and the current LastIndexedOffset.
            return Futures.failedFuture(new DataCorruptionException(String.format(
                    "%s: '%s' has CompactionStartOffset=%s and CompactionLength=%s.",
                    this.traceLogId, this.metadata.getName(), startOffset, maxLength)));
        } else if (maxLength == 0) {
            // Nothing to do.
            log.debug("{}: Up to date.", this.traceLogId);
            return CompletableFuture.completedFuture(null);
        }

        // Read the Table Entries beginning at the specified offset, without exceeding the given maximum length.
        return getRetryPolicy().runAsync(
                () -> readCandidates(startOffset, maxLength, timer)
                        .thenComposeAsync(candidates -> excludeObsolete(candidates, timer)
                                        .thenComposeAsync(v -> copyCandidates(candidates, timer), this.executor),
                                this.executor),
                this.executor);
    }

    /**
     * Reads a set of compaction candidates from the Segment and generates a {@link CompactionArgs} with them grouped
     * by their Key Hash.
     *
     * @param startOffset The offset to start reading from.
     * @param maxLength   The maximum number of bytes to read. The actual number of bytes read will be at most this, since
     *                    we can only read whole Table Entries.
     * @param timer       Timer for the operation.
     * @return A CompletableFuture that, when completed, will contain a {@link CompactionArgs} with the result.
     */
    private CompletableFuture<CompactionArgs> readCandidates(long startOffset, int maxLength, TimeoutTimer timer) {
        ReadResult rr = this.segment.read(startOffset, maxLength, timer.getRemaining());
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
        val result = newCompactionArgs(startOffset);
        final long maxOffset = startOffset + maxLength;
        val input = inputData.getBufferViewReader();
        try {
            while (result.getEndOffset() < maxOffset) {
                val e = AsyncTableEntryReader.readEntryComponents(input, result.getEndOffset(), SERIALIZER);

                // We only care about updates, and not removals.
                if (!e.getHeader().isDeletion()) {
                    // Deduplicate (based on key).
                    result.add(new Candidate(result.getEndOffset(), TableEntry.versioned(e.getKey(), e.getValue(), e.getVersion())));
                }

                // Every entry, even if deleted or duplicated, must be counted, as we will need to adjust the Segment's
                // TOTAL_ENTRY_COUNT attribute at the end.
                result.entryProcessed(e.getHeader().getTotalLength());
            }
        } catch (BufferView.Reader.OutOfBoundsException ex) {
            // We chose an arbitrary compact length, so it is quite possible we stopped reading in the middle of an entry.
            // As such, BufferView.Reader.OutOfBoundsException is the only way to know when to stop. When this happens,
            // we will have collected the total compact length in segmentOffset.
        }

        return result;
    }

    protected abstract CompletableFuture<Void> excludeObsolete(CompactionArgs args, TimeoutTimer timer);

    /**
     * Copies the {@link Candidate}s in the given {@link CompactionArgs} set to a contiguous block at the end of the Segment.
     *
     * @param args  A {@link CompactionArgs} containing the {@link Candidate}s to copy.
     * @param timer Timer for the operation.
     * @return A CompletableFuture that, when completed, indicate the candidates have been copied.
     */
    private CompletableFuture<Void> copyCandidates(CompactionArgs args, TimeoutTimer timer) {
        val attributes = generateAttributeUpdates(args);

        // Collect all the candidates for copying and calculate the total serialization length.
        val toWrite = new ArrayList<TableEntry>();
        val totalLength = new AtomicInteger(0);
        args.getAll().stream().sorted(Comparator.comparingLong(c -> c.entry.getKey().getVersion()))
                .forEach(c -> {
                    toWrite.add(c.entry);
                    generateIndexUpdates(c, totalLength.get(), attributes);
                    totalLength.addAndGet(SERIALIZER.getUpdateLength(c.entry));
                });

        // Generate the necessary AttributeUpdates that will need to be applied regardless of whether we copy anything or not.
        CompletableFuture<?> result;
        if (totalLength.get() == 0) {
            // Nothing to do; update the necessary segment attributes.
            assert toWrite.size() == 0;
            result = this.segment.updateAttributes(attributes, timer.getRemaining());
        } else {
            // Perform a Segment Append with re-serialized entries (Explicit versions), and atomically update the necessary
            // segment attributes.
            BufferView appendData = SERIALIZER.serializeUpdateWithExplicitVersion(toWrite);
            result = this.segment.append(appendData, attributes, timer.getRemaining());
            log.debug("{}: Compacting {}, CopyCount={}, CopyLength={}.", this.traceLogId, args, toWrite.size(), totalLength);
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
    private AttributeUpdateCollection generateAttributeUpdates(CompactionArgs candidates) {
        return AttributeUpdateCollection.from(
                new AttributeUpdate(TableAttributes.COMPACTION_OFFSET, AttributeUpdateType.ReplaceIfEquals, candidates.getEndOffset(), candidates.getStartOffset()),
                new AttributeUpdate(TableAttributes.TOTAL_ENTRY_COUNT, AttributeUpdateType.Accumulate, calculateTotalEntryDelta(candidates)));
    }

    /**
     * When implemented in a derived class, calculates a delta (positive, zero or negative) which indicates by how much
     * should the {@link TableAttributes#TOTAL_ENTRY_COUNT} be adjusted if the compaction is successful.
     *
     * @param candidates The Candidates for compaction.
     * @return The {@link TableAttributes#TOTAL_ENTRY_COUNT} delta as a result of compaction.
     */
    protected abstract int calculateTotalEntryDelta(CompactionArgs candidates);

    protected void generateIndexUpdates(Candidate c, int batchOffset, AttributeUpdateCollection indexUpdates) {
        // Default implementation intentionally left blank. Derived classes may override this.
    }

    /**
     * Gets the retry policy to use for compaction.
     *
     * @return The Retry Policy to use.
     */
    protected Retry.RetryAndThrowBase<Exception> getRetryPolicy() {
        return Retry.NO_RETRY;
    }

    /**
     * Calculates the Segment Offset where to start the compaction at. This is the maximum of the Segment's
     * {@link TableAttributes#COMPACTION_OFFSET} attribute and the Segment's StartOffset (where it is truncated).
     *
     * @return The Segment Offset where to begin compaction at.
     */
    private long getCompactionStartOffset() {
        return Math.max(IndexReader.getCompactionOffset(this.metadata), this.metadata.getStartOffset());
    }

    //region Helper Classes

    protected static class CompactionArgs {
        /**
         * Offset where compaction began from.
         */
        @Getter
        private final long startOffset;

        /**
         * Last offset that was processed for compaction. This will lie at a TableEntry's boundary and indicates the
         * location where the next compaction can begin from.
         */
        @Getter
        private long endOffset;

        /**
         * Number of Table Entries processed, including duplicates, obsoletes and removals.
         */
        @Getter
        private int count;

        /**
         * Candidates, indexed by their TableKey.
         */
        private final Map<BufferView, Candidate> candidatesByKey = new HashMap<>();

        CompactionArgs(long startOffset) {
            this.startOffset = startOffset;
            this.endOffset = this.startOffset;
            this.count = 0;
        }

        void entryProcessed(int serializationLength) {
            this.endOffset += serializationLength;
            this.count++;
        }

        /**
         * Adds the given {@link Candidate}, but only if its Key does not exist or exists with a lower version.
         *
         * @param c The {@link Candidate} to add.
         */
        boolean add(Candidate c) {
            val key = c.entry.getKey().getKey();
            val existing = this.candidatesByKey.get(key);
            if (existing == null || existing.entry.getKey().getVersion() < c.entry.getKey().getVersion()) {
                // Either first time seeing this key or we saw it before with a smaller version - use this one.
                this.candidatesByKey.put(key, c);
                return true;
            }

            return false;
        }

        void remove(Candidate candidate) {
            this.candidatesByKey.remove(candidate.entry.getKey().getKey());
        }

        void removeAll(Collection<Candidate> candidates) {
            candidates.forEach(this::remove);
        }

        /**
         * Excludes any {@link Candidate}s from the {@link CompactionArgs} that match the given key but have
         *
         * @param existingKey       A {@link TableKey} that exists and is included in the index.
         * @param existingKeyOffset The existing Key's offset.
         */
        void handleExistingKey(TableKey existingKey, long existingKeyOffset) {
            val key = existingKey.getKey();
            val c = this.candidatesByKey.get(key);
            if (c != null && c.segmentOffset < existingKeyOffset) {
                this.candidatesByKey.remove(key);
            }
        }

        /**
         * Gets an unordered collection of {@link Candidate}s in this set.
         *
         * @return The result.
         */
        Collection<Candidate> getAll() {
            return this.candidatesByKey.values();
        }

        int getCopyCandidateCount() {
            return this.candidatesByKey.size();
        }

        @Override
        public String toString() {
            return String.format("StartOffset=%s, EndOffset=%s, ProcessedCount=%s, CandidateCount=%s",
                    this.startOffset, this.endOffset, this.count, this.candidatesByKey.size());
        }
    }

    @RequiredArgsConstructor
    protected static class Candidate {
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

    @Data
    static class Config {
        private final int maxCompactionSize;
    }

    //endregion
}
