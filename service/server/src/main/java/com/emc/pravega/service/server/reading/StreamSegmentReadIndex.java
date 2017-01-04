/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.reading;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.util.AvlTreeIndex;
import com.emc.pravega.common.util.ByteArraySegment;
import com.emc.pravega.common.util.SortedIndex;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.ReadResultEntry;
import com.emc.pravega.service.contracts.ReadResultEntryContents;
import com.emc.pravega.service.contracts.ReadResultEntryType;
import com.emc.pravega.service.contracts.StreamSegmentSealedException;
import com.emc.pravega.service.server.CacheKey;
import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.ReadOnlyStorage;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Read Index for a single StreamSegment. Integrates reading data from the following sources:
 * <ol>
 * <li> The tail-end part of the StreamSegment (the part that is in DurableLog, but not yet in Storage).
 * <li> The part of the StreamSegment that is in Storage, but not in DurableLog. This data will be brought into memory
 * for fast read-ahead access.
 * <li> Future appends. If a read operation requests data from an offset in the future, the read operation will block until
 * data becomes available or until it gets canceled.
 * </ol>
 */
@Slf4j
class StreamSegmentReadIndex implements CacheManager.Client, AutoCloseable {
    //region Members

    private final String traceObjectId;
    private final SortedIndex<Long, ReadIndexEntry> indexEntries; // Key = Last Offset of Entry, Value = Entry.
    private final ReadIndexConfig config;
    private final Cache cache;
    private final FutureReadResultEntryCollection futureReads;
    private final HashMap<Long, Long> mergeOffsets; //Key = StreamSegmentId (Merged), Value = Merge offset.
    private final StorageReader storageReader;
    private final ReadIndexSummary summary;
    private final ScheduledExecutorService executor;
    private SegmentMetadata metadata;
    private long lastAppendedOffset;
    private boolean recoveryMode;
    private boolean closed;
    private boolean merged;
    private final Object lock = new Object();

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentReadIndex class.
     *
     * @param config       Read Index Configuration.
     * @param metadata     The StreamSegmentMetadata to use.
     * @param cache        The Cache to use to store, read and manage data entries.
     * @param storage      A ReadOnlyStorage to fetch data if not in Cache.
     * @param executor     An executor to run async operations.
     * @param recoveryMode Whether we are in recovery mode at the time of creation (this can change later on).
     * @throws NullPointerException If any of the arguments are null.
     */
    StreamSegmentReadIndex(ReadIndexConfig config, SegmentMetadata metadata, Cache cache, ReadOnlyStorage storage, ScheduledExecutorService executor, boolean recoveryMode) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(metadata, "metadata");
        Preconditions.checkNotNull(cache, "cache");
        Preconditions.checkNotNull(storage, "storage");
        Preconditions.checkNotNull(executor, "executor");

        this.traceObjectId = String.format("ReadIndex[%d-%d]", metadata.getContainerId(), metadata.getId());
        this.config = config;
        this.metadata = metadata;
        this.cache = cache;
        this.recoveryMode = recoveryMode;
        this.indexEntries = new AvlTreeIndex<>(Long::compare);
        this.futureReads = new FutureReadResultEntryCollection();
        this.mergeOffsets = new HashMap<>();
        this.lastAppendedOffset = -1;
        this.storageReader = new StorageReader(metadata, storage, executor);
        this.executor = executor;
        this.summary = new ReadIndexSummary();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;

            // Close storage reader (and thus cancel those reads).
            this.storageReader.close();

            // Cancel future reads.
            this.futureReads.close();
            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region CacheManager.Client Implementation

    @Override
    public CacheManager.CacheStatus getCacheStatus() {
        Exceptions.checkNotClosed(this.closed, this);
        return this.summary.toCacheStatus();
    }

    @Override
    public long updateGenerations(int currentGeneration, int oldestGeneration) {
        Exceptions.checkNotClosed(this.closed, this);

        // Update the current generation with the provided info.
        this.summary.setCurrentGeneration(currentGeneration);

        // Identify & collect those entries that can be removed, then remove them from the index.
        AtomicLong sizeRemoved = new AtomicLong();
        ArrayList<ReadIndexEntry> toRemove = new ArrayList<>();
        synchronized (this.lock) {
            this.indexEntries.forEach(entry -> {
                // We can only evict if both these conditions are met:
                // 1. The entry is a Cache Entry (Redirect entries cannot be removed).
                // 2. Every single byte in the entry has to exist in Storage.
                boolean canRemove = entry.isDataEntry()
                        && entry.getLastStreamSegmentOffset() <= this.metadata.getStorageLength()
                        && entry.getGeneration() < oldestGeneration;
                if (canRemove) {
                    toRemove.add(entry);
                }
            });

            toRemove.forEach(e -> this.indexEntries.remove(e.getLastStreamSegmentOffset()));
        }

        // Remove the cache entries, as well as update the summary (no need for holding the lock here; we are not modifying the index).
        toRemove.forEach(e -> {
            this.cache.remove(getCacheKey(e));
            long entryLength = e.getLength();
            this.summary.remove(entryLength, e.getGeneration());
            sizeRemoved.addAndGet(entryLength);
        });

        return sizeRemoved.get();
    }

    //endregion

    //region Properties

    @Override
    public String toString() {
        return String.format("%s (%s)", this.traceObjectId, this.metadata.getName());
    }

    /**
     * Gets a value indicating whether this Read Index is merged into another one.
     */
    public boolean isMerged() {
        return this.merged;
    }

    /**
     * Marks this Read Index as merged into another one.
     */
    public void markMerged() {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!this.merged, "StreamSegmentReadIndex %d is already merged.", this.metadata.getId());
        log.debug("{}: Merged.", this.traceObjectId);
        this.merged = true;
    }

    private CacheKey getCacheKey(ReadIndexEntry entry) {
        if (entry instanceof MergedReadIndexEntry) {
            MergedReadIndexEntry me = (MergedReadIndexEntry) entry;
            return new CacheKey(me.getSourceSegmentId(), me.getSourceSegmentOffset());
        } else {
            // Return a CacheKey; if a RedirectReadIndexEntry then no data should be available in the cache.
            return new CacheKey(this.metadata.getId(), entry.getStreamSegmentOffset());
        }
    }

    //endregion

    //region Recovery

    /**
     * Puts this Read Index out of recovery mode, while pointing it to the new metadata (Metadata objects can differ
     * between recovery and non-recovery).
     *
     * @param newMetadata The new metadata object to use from now on.
     * @throws IllegalStateException    If the Read Index is not in recovery mode.
     * @throws NullPointerException     If the given metadata is null.
     * @throws IllegalArgumentException If the new metadata does not match the old one perfectly.
     */
    public void exitRecoveryMode(SegmentMetadata newMetadata) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.recoveryMode, "Read Index is not in recovery mode.");
        Preconditions.checkNotNull(newMetadata, "newMetadata");
        Exceptions.checkArgument(newMetadata.getId() == this.metadata.getId(), "newMetadata", "New Metadata StreamSegmentId is different from existing one.");
        Exceptions.checkArgument(newMetadata.getDurableLogLength() == this.metadata.getDurableLogLength(), "newMetadata", "New Metadata DurableLogLength is different from existing one.");
        Exceptions.checkArgument(newMetadata.getStorageLength() == this.metadata.getStorageLength(), "newMetadata", "New Metadata StorageLength is different from existing one.");
        Exceptions.checkArgument(newMetadata.isSealed() == this.metadata.isSealed(), "newMetadata", "New Metadata Sealed Flag is different from existing one.");
        Exceptions.checkArgument(newMetadata.isMerged() == this.metadata.isMerged(), "newMetadata", "New Metadata Merged Flag is different from existing one.");
        Exceptions.checkArgument(newMetadata.isDeleted() == this.metadata.isDeleted(), "newMetadata", "New Metadata Deletion Flag is different from existing one.");

        this.metadata = newMetadata;
        this.recoveryMode = false;
        log.info("{}: Exit RecoveryMode.", this.traceObjectId);
    }

    //endregion

    //region Appending

    /**
     * Appends the given range of bytes at the given offset.
     *
     * @param offset The offset within the StreamSegment to append at.
     * @param data   The range of bytes to append.
     * @throws NullPointerException     If data is null.
     * @throws IllegalArgumentException If the operation would cause writing beyond the StreamSegment's DurableLogLength.
     * @throws IllegalArgumentException If the offset is invalid (does not match the previous append offset).
     */
    public void append(long offset, byte[] data) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!isMerged(), "StreamSegment has been merged into a different one. Cannot append more ReadIndex entries.");

        if (data.length == 0) {
            // Nothing to do. Adding empty read entries will only make our system slower and harder to debug.
            return;
        }

        // Metadata check can be done outside the write lock.
        // Adding at the end means that we always need to "catch-up" with DurableLogLength. Check to see if adding
        // this entry will make us catch up to it or not.
        long durableLogLength = this.metadata.getDurableLogLength();
        long endOffset = offset + data.length;
        Exceptions.checkArgument(endOffset <= durableLogLength, "offset", "The given range of bytes (%d-%d) is beyond the StreamSegment Durable Log Length (%d).", offset, endOffset, durableLogLength);

        // Then append an entry for it in the ReadIndex.
        this.cache.insert(new CacheKey(this.metadata.getId(), offset), data);
        appendEntry(new ReadIndexEntry(offset, data.length));
    }

    /**
     * Executes Step 1 of the 2-Step Merge Process.
     * The StreamSegments are merged (Source->Target@Offset) in Metadata and a ReadIndex Redirection is put in place.
     * At this stage, the Source still exists as a physical object in Storage, and we need to keep its ReadIndex around, pointing
     * to the old object.
     *
     * @param offset                   The offset within the StreamSegment to merge at.
     * @param sourceStreamSegmentIndex The Read Index to begin merging.
     * @throws NullPointerException     If data is null.
     * @throws IllegalStateException    If the current StreamSegment is a child StreamSegment.
     * @throws IllegalArgumentException If the operation would cause writing beyond the StreamSegment's DurableLogLength.
     * @throws IllegalArgumentException If the offset is invalid (does not match the previous append offset).
     * @throws IllegalArgumentException If sourceStreamSegmentIndex refers to a StreamSegment that is already merged.
     * @throws IllegalArgumentException If sourceStreamSegmentIndex refers to a StreamSegment that has a different parent
     *                                  StreamSegment than the current index's one.
     */
    public void beginMerge(long offset, StreamSegmentReadIndex sourceStreamSegmentIndex) {
        long traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "beginMerge", offset, sourceStreamSegmentIndex.traceObjectId);
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(this.metadata.getParentId() == ContainerMetadata.NO_STREAM_SEGMENT_ID, "Cannot merge a StreamSegment into a child StreamSegment.");
        Exceptions.checkArgument(!sourceStreamSegmentIndex.isMerged(), "sourceStreamSegmentIndex", "Given StreamSegmentReadIndex is already merged.");

        SegmentMetadata sourceMetadata = sourceStreamSegmentIndex.metadata;
        Exceptions.checkArgument(sourceMetadata.getParentId() == this.metadata.getId(), "sourceStreamSegmentIndex", "Given StreamSegmentReadIndex refers to a StreamSegment that does not have this ReadIndex's StreamSegment as a parent.");
        Exceptions.checkArgument(sourceMetadata.isSealed(), "sourceStreamSegmentIndex", "Given StreamSegmentReadIndex refers to a StreamSegment that is not sealed.");

        long sourceLength = sourceMetadata.getDurableLogLength();
        if (sourceLength == 0) {
            // Nothing to do.
            return;
        }

        // Metadata check can be done outside the write lock.
        // Adding at the end means that we always need to "catch-up" with DurableLogLength. Check to see if adding
        // this entry will make us catch up to it or not.
        long durableLogLength = this.metadata.getDurableLogLength();
        long endOffset = offset + sourceLength;
        Exceptions.checkArgument(endOffset <= durableLogLength, "offset", "The given range of bytes(%d-%d) is beyond the StreamSegment Durable Log Length (%d).", offset, endOffset, durableLogLength);

        // Check and record the merger (optimistically).
        RedirectReadIndexEntry newEntry = new RedirectReadIndexEntry(offset, sourceLength, sourceStreamSegmentIndex);
        synchronized (this.lock) {
            Exceptions.checkArgument(!this.mergeOffsets.containsKey(sourceMetadata.getId()), "sourceStreamSegmentIndex", "Given StreamSegmentReadIndex is already merged or in the process of being merged into this one.");
            this.mergeOffsets.put(sourceMetadata.getId(), newEntry.getLastStreamSegmentOffset());
        }

        try {
            appendEntry(newEntry);
        } catch (Exception ex) {
            // If the merger failed, roll back the markers.
            synchronized (this.lock) {
                this.mergeOffsets.remove(sourceMetadata.getId());
            }

            throw ex;
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "beginMerge", traceId);
    }

    /**
     * Executes Step 2 of the 2-Step Merge Process.
     * The StreamSegments are physically merged in the Storage. The Source StreamSegment does not exist anymore.
     * The ReadIndex entries of the two Streams are actually joined together.
     *
     * @param sourceSegmentStreamId The Id of the StreamSegment that was merged into this one.
     */
    public void completeMerge(long sourceSegmentStreamId) {
        long traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "completeMerge", sourceSegmentStreamId);
        Exceptions.checkNotClosed(this.closed, this);

        // Find the appropriate redirect entry.
        RedirectReadIndexEntry redirectEntry;
        long endOffset;
        synchronized (this.lock) {
            endOffset = this.mergeOffsets.getOrDefault(sourceSegmentStreamId, -1L);
            Exceptions.checkArgument(endOffset >= 0, "sourceSegmentStreamId", "Given StreamSegmentReadIndex's merger with this one has not been initiated using beginMerge. Cannot finalize the merger.");

            // Get the RedirectReadIndexEntry. These types of entries are sticky in the cache and DO NOT contribute to the
            // cache Stats. They are already accounted for in the other Segment's ReadIndex.
            Optional<ReadIndexEntry> treeEntry = this.indexEntries.get(endOffset);
            assert treeEntry.isPresent() && (treeEntry.get() instanceof RedirectReadIndexEntry) : String.format("mergeOffsets points to a ReadIndexEntry that does not exist or is of the wrong type. sourceStreamSegmentId = %d, offset = %d, treeEntry = %s.", sourceSegmentStreamId, endOffset, treeEntry);
            redirectEntry = (RedirectReadIndexEntry) treeEntry.get();
        }

        StreamSegmentReadIndex sourceIndex = redirectEntry.getRedirectReadIndex();
        SegmentMetadata sourceMetadata = sourceIndex.metadata;
        Exceptions.checkArgument(sourceMetadata.isDeleted(), "sourceSegmentStreamId", "Given StreamSegmentReadIndex refers to a StreamSegment that has not been deleted yet.");

        // Get all the entries from the source index and append them here.
        List<ReadIndexEntry> sourceEntries = sourceIndex.getAllEntries(redirectEntry.getStreamSegmentOffset());

        synchronized (this.lock) {
            // Remove redirect entry (again, no need to update the Cache Stats, as this is a RedirectReadIndexEntry).
            this.indexEntries.remove(endOffset);
            this.mergeOffsets.remove(sourceSegmentStreamId);
            sourceEntries.forEach(this::addToIndex);
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "completeMerge", traceId);
    }

    private void appendEntry(ReadIndexEntry entry) {
        log.debug("{}: Append (Offset = {}, Length = {}).", this.traceObjectId, entry.getStreamSegmentOffset(), entry.getLength());

        synchronized (this.lock) {
            Exceptions.checkArgument(this.lastAppendedOffset < 0 || entry.getStreamSegmentOffset() == this.lastAppendedOffset + 1, "entry", "The given range of bytes (%d-%d) does not start right after the last appended range (%d).", entry.getStreamSegmentOffset(), entry.getLastStreamSegmentOffset(), this.lastAppendedOffset);

            // Finally, append the entry.
            // Key is Offset + Length -1 = Last Offset Of Entry. Value is entry itself. This makes searching easier.
            Optional<ReadIndexEntry> oldEntry = addToIndex(entry);
            assert !oldEntry.isPresent() : String.format("Added a new entry in the ReadIndex that overrode an existing element. New = %s, Old = %s.", entry, oldEntry);
            this.lastAppendedOffset = entry.getLastStreamSegmentOffset();
        }
    }

    private void insert(long offset, ByteArraySegment data) {
        log.debug("{}: Insert (Offset = {}, Length = {}).", this.traceObjectId, offset, data.getLength());

        // There is a very small chance we might be adding data twice, if we get two concurrent requests that slipped past
        // the StorageReader. Fixing it would be complicated, so let's see if it poses any problems.
        this.cache.insert(new CacheKey(this.metadata.getId(), offset), data);
        ReadIndexEntry entry = new ReadIndexEntry(offset, data.getLength());
        Optional<ReadIndexEntry> oldEntry;
        synchronized (this.lock) {
            Exceptions.checkArgument(entry.getLastStreamSegmentOffset() < this.metadata.getStorageLength(), "entry", "The given range of bytes (%d-%d) does not correspond to the StreamSegment range that is in Storage (%d).", entry.getStreamSegmentOffset(), entry.getLastStreamSegmentOffset(), this.metadata.getStorageLength());
            oldEntry = addToIndex(entry);
        }

        if (oldEntry.isPresent()) {
            log.warn("{}: Insert overrode existing entry (Offset = {}, OldLength = {}, NewLength = {}).", this.traceObjectId, entry.getStreamSegmentOffset(), entry.getLength(), oldEntry.get().getLength());
        }
    }

    private Optional<ReadIndexEntry> addToIndex(ReadIndexEntry entry) {
        // Insert the new entry and figure out if an old entry was overwritten.
        Optional<ReadIndexEntry> oldEntry = this.indexEntries.put(entry);

        if (entry.isDataEntry()) {
            if (entry instanceof MergedReadIndexEntry) {
                // This entry has already existed in the cache for a while; do not change its generation.
                this.summary.add(entry.getLength(), entry.getGeneration());
            } else {
                // Update the Stats with the entry's length, and set the entry's generation as well.
                int generation = this.summary.add(entry.getLength());
                entry.setGeneration(generation);
            }
        }

        if (oldEntry.isPresent() && oldEntry.get().isDataEntry()) {
            // Need to eject the old entry's data from the Cache Stats.
            this.summary.remove(oldEntry.get().getLength(), oldEntry.get().getGeneration());
        }

        return oldEntry;
    }

    //endregion

    //region Reading

    /**
     * Triggers all future reads that have a starting offset before the given value.
     *
     * @throws IllegalStateException If the read index is in recovery mode.
     */
    public void triggerFutureReads() {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!this.recoveryMode, "StreamSegmentReadIndex is in Recovery Mode.");

        // Get all eligible Future Reads which wait for data prior to the end offset.
        // Since we are not actually using this entry's data, there is no need to 'touch' it.
        Optional<ReadIndexEntry> lastEntry = this.indexEntries.getLast();
        if (!lastEntry.isPresent()) {
            // Nothing to do.
            return;
        }

        Collection<FutureReadResultEntry> futureReads;
        boolean sealed = this.metadata.isSealed();
        if (sealed) {
            // Get everything, even if some Future Reads are in the future - those will eventually return EndOfSegment.
            futureReads = this.futureReads.pollAll();
        } else {
            // Get only those up to the last offset of the last append.
            futureReads = this.futureReads.poll(lastEntry.get().getLastStreamSegmentOffset());
        }

        log.debug("{}: triggerFutureReads (Count = {}, Sealed = {}).", this.traceObjectId, futureReads.size(), sealed);

        for (FutureReadResultEntry r : futureReads) {
            ReadResultEntry entry = getFirstReadResultEntry(r.getStreamSegmentOffset(), r.getRequestedReadLength());
            assert entry != null : "Serving a StorageReadResultEntry with a null result";
            assert !(entry instanceof FutureReadResultEntry) : "Serving a FutureReadResultEntry with another FutureReadResultEntry.";

            log.trace("{}: triggerFutureReads (Offset = {}, Type = {}).", this.traceObjectId, r.getStreamSegmentOffset(), entry.getType());
            if (entry.getType() == ReadResultEntryType.EndOfStreamSegment) {
                // We have attempted to read beyond the end of the stream. Fail the read request with the appropriate message.
                r.fail(new StreamSegmentSealedException(String.format("StreamSegment has been sealed at offset %d. There can be no more reads beyond this offset.", this.metadata.getDurableLogLength())));
            } else {
                entry.getContent().thenAccept(r::complete);
            }
        }
    }

    /**
     * Reads a range of bytes from the StreamSegment.
     *
     * @param startOffset The offset in the StreamSegment where to start reading.
     * @param maxLength   The maximum number of bytes to read.
     * @param timeout     Timeout for the operation.
     * @return A ReadResult containing methods for retrieving the result.
     * @throws IllegalStateException    If the read index is in recovery mode.
     * @throws IllegalArgumentException If the parameters are invalid.
     * @throws IllegalArgumentException If the StreamSegment is sealed and startOffset is beyond its length.
     */
    public ReadResult read(long startOffset, int maxLength, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!this.recoveryMode, "StreamSegmentReadIndex is in Recovery Mode.");
        Exceptions.checkArgument(startOffset >= 0, "startOffset", "startOffset must be a non-negative number.");
        Exceptions.checkArgument(maxLength >= 0, "maxLength", "maxLength must be a non-negative number.");
        Exceptions.checkArgument(canReadAtOffset(startOffset), "startOffset", "StreamSegment is sealed and startOffset is beyond the last offset of the StreamSegment.");

        log.debug("{}: Read (Offset = {}, MaxLength = {}).", this.traceObjectId, startOffset, maxLength);
        return new StreamSegmentReadResult(startOffset, maxLength, this::getFirstReadResultEntry, this.traceObjectId);
    }

    private boolean canReadAtOffset(long offset) {
        // We can only read at a particular offset if:
        // * The segment is not sealed (we are allowed to do a future read) OR
        // * The segment is sealed and we are not trying to read from the last offset.
        return !this.metadata.isSealed() || offset < this.metadata.getDurableLogLength();
    }

    /**
     * Returns the first ReadResultEntry that matches the specified search parameters.
     *
     * @param resultStartOffset The Offset within the StreamSegment where to start returning data from.
     * @param maxLength         The maximum number of bytes to return.
     * @return A ReadResultEntry representing the data to return.
     */
    private CompletableReadResultEntry getFirstReadResultEntry(long resultStartOffset, int maxLength) {
        Exceptions.checkNotClosed(this.closed, this);

        if (maxLength < 0) {
            // Nothing to read.
            return null;
        }

        // Check to see if we are trying to read beyond the last offset of a sealed StreamSegment.
        if (!canReadAtOffset(resultStartOffset)) {
            return new EndOfStreamSegmentReadResultEntry(resultStartOffset, maxLength);
        }

        CompletableReadResultEntry result;
        synchronized (this.lock) {
            if (this.indexEntries.size() == 0) {
                // We have no entries in the Read Index.
                // Use the metadata to figure out whether to return a Storage or Future Read.
                result = createDataNotAvailableRead(resultStartOffset, maxLength);
            } else {
                // We have at least one entry.
                // Find the first entry that has an End offset beyond equal to at least ResultStartOffset.
                Optional<ReadIndexEntry> ceilingEntry = this.indexEntries.getCeiling(resultStartOffset);
                if (!ceilingEntry.isPresent()) {
                    // The ResultStartOffset is beyond the End Offset of the last entry in the index.
                    // Use the metadata to figure out whether to return a Storage or Future Read, since we do not have
                    // this data in memory.
                    result = createDataNotAvailableRead(resultStartOffset, maxLength);
                } else {
                    // We have an entry. Let's see if it's valid or not.
                    ReadIndexEntry treeEntry = ceilingEntry.get();
                    if (resultStartOffset < treeEntry.getStreamSegmentOffset()) {
                        // ResultStartOffset is before the Start Offset of this entry. This means either:
                        // 1. This is the first entry and ResultStartOffset is before it. OR
                        // 2. We have a gap in our entries, and ResultStartOffset is somewhere in there.
                        // We must issue a Storage Read to bring the data to us (with a readLength of up to the size of the gap).
                        int readLength = (int) Math.min(maxLength, treeEntry.getStreamSegmentOffset() - resultStartOffset);
                        result = createStorageRead(resultStartOffset, readLength);
                    } else if (treeEntry instanceof RedirectReadIndexEntry) {
                        result = getRedirectedReadResultEntry(resultStartOffset, maxLength, (RedirectReadIndexEntry) treeEntry);
                    } else {
                        // ResultStartOffset is after the StartOffset and before the End Offset of this entry.
                        result = createMemoryRead(treeEntry, resultStartOffset, maxLength);
                    }
                }
            }
        }

        // Just before exiting, check we are returning something. We should always return something if we coded this correctly.
        assert result != null : String.format("Reached the end of getFirstReadResultEntry(id=%d, offset=%d, length=%d) with no plausible result in sight. This means we missed a case.", this.metadata.getId(), resultStartOffset, maxLength);
        return result;
    }

    private CompletableReadResultEntry getRedirectedReadResultEntry(long streamSegmentOffset, int maxLength, RedirectReadIndexEntry entry) {
        StreamSegmentReadIndex redirectedIndex = entry.getRedirectReadIndex();
        long redirectOffset = streamSegmentOffset - entry.getStreamSegmentOffset();
        long entryLength = entry.getLength();
        assert redirectOffset >= 0 && redirectOffset < entryLength :
                String.format("Redirected offset would be outside of the range of the Redirected StreamSegment. StreamSegmentOffset = %d, MaxLength = %d, Entry.StartOffset = %d, Entry.Length = %d, RedirectOffset = %d.",
                        streamSegmentOffset,
                        maxLength,
                        entry.getStreamSegmentOffset(),
                        entryLength,
                        redirectOffset);

        if (entryLength < maxLength) {
            maxLength = (int) entryLength;
        }

        CompletableReadResultEntry result = redirectedIndex.getFirstReadResultEntry(redirectOffset, maxLength);
        if (result != null) {
            // Since this is a redirect to a (merged) Transaction, it is possible that between now and when the caller
            // invokes the requestContent() on the entry the Transaction may be fully merged (in Tier2). If that's the
            // case, then this entry will fail with either ObjectClosedException or StreamSegmentNotFoundException, since
            // it is pointing to the now defunct Transaction segment. At that time, a simple retry of the read would
            // yield the right result. However, in order to recover from this without the caller's intervention, we pass
            // a pointer to getFirstReadResultEntry to the RedirectedReadResultEntry in case it fails with such an exception;
            // that class has logic in it to invoke it if needed and get the right entry.
            result = new RedirectedReadResultEntry(result, entry.getStreamSegmentOffset(), this::getFirstReadResultEntry, this.executor);
        }

        return result;
    }

    /**
     * Creates a ReadResultEntry that is a placeholder for data that is not currently available in memory.
     *
     * @param streamSegmentOffset The Offset in the StreamSegment where to the ReadResultEntry starts at.
     * @param maxLength           The maximum length of the Read, from the Offset of this ReadResultEntry.
     */
    private ReadResultEntryBase createDataNotAvailableRead(long streamSegmentOffset, int maxLength) {
        long storageLength = this.metadata.getStorageLength();
        if (streamSegmentOffset < storageLength) {
            // Requested data exists in Storage.
            // Determine actual read length (until Storage Length) and make sure it does not exceed maxLength.
            long actualReadLength = storageLength - streamSegmentOffset;
            if (actualReadLength > maxLength) {
                actualReadLength = maxLength;
            }

            return createStorageRead(streamSegmentOffset, (int) actualReadLength);
        } else {
            return createFutureRead(streamSegmentOffset, maxLength);
        }
    }

    /**
     * Creates a ReadResultEntry for data that is readily available in memory.
     *
     * @param entry               The ReadIndexEntry to use.
     * @param streamSegmentOffset The Offset in the StreamSegment where to the ReadResultEntry starts at.
     * @param maxLength           The maximum length of the Read, from the Offset of this ReadResultEntry.
     */
    private ReadResultEntryBase createMemoryRead(ReadIndexEntry entry, long streamSegmentOffset, int maxLength) {
        assert streamSegmentOffset >= entry.getStreamSegmentOffset() : String.format("streamSegmentOffset{%d} < entry.getStreamSegmentOffset{%d}", streamSegmentOffset, entry.getStreamSegmentOffset());

        int entryOffset = (int) (streamSegmentOffset - entry.getStreamSegmentOffset());

        int length = (int) Math.min(maxLength, entry.getLength() - entryOffset);
        assert length > 0 : String.format("length{%d} <= 0. streamSegmentOffset = %d, maxLength = %d, entry.offset = %d, entry.length = %d", length, streamSegmentOffset, maxLength, entry.getStreamSegmentOffset(), entry.getLength());
        byte[] data = this.cache.get(getCacheKey(entry));
        assert data != null : String.format("No Cache Entry could be retrieved for entry %s", entry);

        // Update its generation before returning it.
        int generation = this.summary.touchOne(entry.getGeneration());
        entry.setGeneration(generation);
        return new CacheReadResultEntry(entry.getStreamSegmentOffset(), data, entryOffset, length);
    }

    /**
     * Creates a ReadResultEntry that is a placeholder for data that is not in memory, but exists in Storage.
     *
     * @param streamSegmentOffset The Offset in the StreamSegment where to the ReadResultEntry starts at.
     * @param readLength          The maximum length of the Read, from the Offset of this ReadResultEntry.
     */
    private ReadResultEntryBase createStorageRead(long streamSegmentOffset, int readLength) {
        return new StorageReadResultEntry(streamSegmentOffset, readLength, this::queueStorageRead);
    }

    private void queueStorageRead(long offset, int length, Consumer<ReadResultEntryContents> successCallback, Consumer<Throwable> failureCallback, Duration timeout) {
        // Create a callback that inserts into the ReadIndex (and cache) and invokes the success callback.
        Consumer<StorageReader.Result> doneCallback = result -> {
            ByteArraySegment data = result.getData();
            if (!result.isDerived()) {
                // Only insert primary results into the cache. Derived results are always sub-portions of primaries
                // and there is no need to insert them too, as they are already contained within.
                insert(offset, data);
            }

            successCallback.accept(new ReadResultEntryContents(data.getReader(), data.getLength()));
        };

        // Queue the request for async processing.
        length = getReadAlignedLength(offset, length);
        this.storageReader.execute(new StorageReader.Request(offset, length, doneCallback, failureCallback, timeout));
    }

    /**
     * Returns an adjusted read length based on the given input, making sure the end of the Read Request is aligned with
     * a multiple of STORAGE_READ_MAX_LEN.
     *
     * @param offset     The read offset.
     * @param readLength The requested read length.
     * @return The adjusted (aligned) read length.
     */
    private int getReadAlignedLength(long offset, int readLength) {
        // Calculate how many bytes over the last alignment marker the offset is.
        int lengthSinceLastMultiple = (int) (offset % this.config.getStorageReadMaxLength());

        // Even though we were asked to read a number of bytes, in some cases we will return fewer bytes than requested
        // in order to read-align the reads. Calculate the aligned read length, taking into account the Max and Min
        // number of bytes we are allowed to read.
        return Math.min(readLength, Math.max(this.config.getStorageReadMinLength(), this.config.getStorageReadMaxLength() - lengthSinceLastMultiple));
    }

    /**
     * Creates a ReadResultEntry that is a placeholder for data that is not in memory, or in storage, which has
     * a starting offset beyond the length of the StreamSegment.
     *
     * @param streamSegmentOffset The Offset in the StreamSegment where to the ReadResultEntry starts at.
     * @param maxLength           The maximum length of the Read, from the Offset of this ReadResultEntry.
     */
    private ReadResultEntryBase createFutureRead(long streamSegmentOffset, int maxLength) {
        FutureReadResultEntry entry = new FutureReadResultEntry(streamSegmentOffset, maxLength);
        this.futureReads.add(entry);
        return entry;
    }

    /**
     * Gets a copy of all the ReadIndexEntries in this Index that are not RedirectReadIndices. All returned
     * entries have their offsets adjusted by the given amount.
     *
     * @param offsetAdjustment The amount to adjust the offset by.
     */
    private List<ReadIndexEntry> getAllEntries(long offsetAdjustment) {
        Exceptions.checkArgument(offsetAdjustment >= 0, "offsetAdjustment", "offsetAdjustment must be a non-negative number.");

        synchronized (this.lock) {
            List<ReadIndexEntry> result = new ArrayList<>(this.indexEntries.size());
            this.indexEntries.forEach(entry -> {
                if (entry.isDataEntry()) {
                    result.add(new MergedReadIndexEntry(entry.getStreamSegmentOffset() + offsetAdjustment, this.metadata.getId(), entry));
                }
            });

            return result;
        }
    }

    //endregion
}

