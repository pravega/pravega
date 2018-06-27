/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.reading;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import io.pravega.common.Exceptions;
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.AvlTreeIndex;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.SortedIndex;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import io.pravega.segmentstore.contracts.StreamSegmentSealedException;
import io.pravega.segmentstore.server.CacheManager;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.ReadOnlyStorage;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

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
@ThreadSafe
class StreamSegmentReadIndex implements CacheManager.Client, AutoCloseable {
    //region Members

    private final String traceObjectId;
    @GuardedBy("lock")
    private final SortedIndex<ReadIndexEntry> indexEntries;
    private final ReadIndexConfig config;
    @GuardedBy("lock")
    private final Cache cache;
    private final FutureReadResultEntryCollection futureReads;
    @GuardedBy("lock")
    private final HashMap<Long, PendingMerge> pendingMergers; //Key = Source Segment Id, Value = Pending Merge Info.
    private final StorageReadManager storageReadManager;
    private final ReadIndexSummary summary;
    private final ScheduledExecutorService executor;
    private SegmentMetadata metadata;
    @GuardedBy("lock")
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
        this.indexEntries = new AvlTreeIndex<>();
        this.futureReads = new FutureReadResultEntryCollection();
        this.pendingMergers = new HashMap<>();
        this.lastAppendedOffset = -1;
        this.storageReadManager = new StorageReadManager(metadata, storage, executor);
        this.executor = executor;
        this.summary = new ReadIndexSummary();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        // Quick close (no cache cleanup) this should be used only in case of container shutdown, when the cache will
        // be erased anyway.
        close(false);
    }

    /**
     * Closes the ReadIndex and optionally cleans the cache.
     *
     * @param cleanCache If true, the Cache will be cleaned up of all ReadIndexEntries pertaining to this ReadIndex. If
     *                   false, the Cache will not be touched.
     */
    void close(boolean cleanCache) {
        if (!this.closed) {
            this.closed = true;

            // Close storage reader (and thus cancel those reads).
            this.storageReadManager.close();

            // Cancel registered future reads and any reads pertaining to incomplete mergers.
            ArrayList<Iterator<FutureReadResultEntry>> futureReads = new ArrayList<>();
            futureReads.add(this.futureReads.close().iterator());
            synchronized (this.lock) {
                this.pendingMergers.values().forEach(pm -> futureReads.add(pm.seal().iterator()));
            }
            cancelFutureReads(Iterators.concat(futureReads.iterator()));

            if (cleanCache) {
                this.executor.execute(() -> {
                    removeAllEntries();
                    log.info("{}: Closed.", this.traceObjectId);
                });
            } else {
                log.info("{}: Closed (no cache cleanup).", this.traceObjectId);
            }
        }
    }

    private void cancelFutureReads(Iterator<FutureReadResultEntry> toCancel) {
        CancellationException ce = new CancellationException();
        while (toCancel.hasNext()) {
            FutureReadResultEntry e = toCancel.next();
            if (!e.getContent().isDone()) {
                e.fail(ce);
            }
        }
    }

    /**
     * Removes all entries from the cache and the SortedIndex, regardless of their state.
     *
     * @throws IllegalStateException If the StreamSegmentReadIndex is not closed.
     */
    private void removeAllEntries() {
        // A bit unusual, but we want to make sure we do not call this while the index is active.
        Preconditions.checkState(this.closed, "Cannot call removeAllEntries unless the ReadIndex is closed.");
        int count;
        synchronized (this.lock) {
            this.indexEntries.forEach(entry -> {
                if (entry.isDataEntry()) {
                    CacheKey key = getCacheKey(entry);
                    this.cache.remove(key);
                }
            });

            count = this.indexEntries.size();
            this.indexEntries.clear();
        }

        log.info("{}: Cleared all cache entries ({}).", this.traceObjectId, count);
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
                // In addition, we are free to evict (regardless of Generation, but still subject to the above rules) if
                // every single byte in the entry has been truncated out.
                long lastOffset = entry.getLastStreamSegmentOffset();
                boolean canRemove = entry.isDataEntry()
                        && lastOffset <= this.metadata.getStorageLength()
                        && (entry.getGeneration() < oldestGeneration || lastOffset < this.metadata.getStartOffset());
                if (canRemove) {
                    toRemove.add(entry);
                }
            });

            // Remove from the index and from the cache.
            toRemove.forEach(e -> {
                this.indexEntries.remove(e.key());
                this.cache.remove(getCacheKey(e));
            });
        }

        // Update the summary (no need for holding the lock here; we are not modifying the index).
        toRemove.forEach(e -> {
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
    boolean isMerged() {
        return this.merged;
    }

    /**
     * Marks this Read Index as merged into another one.
     */
    void markMerged() {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!this.merged, "StreamSegmentReadIndex %d is already merged.", this.metadata.getId());
        log.debug("{}: Merged.", this.traceObjectId);
        this.merged = true;
    }

    /**
     * Gets the length of the Segment this ReadIndex refers to.
     */
    long getSegmentLength() {
        return this.metadata.getLength();
    }

    private CacheKey getCacheKey(ReadIndexEntry entry) {
        if (entry instanceof MergedIndexEntry) {
            MergedIndexEntry me = (MergedIndexEntry) entry;
            return new CacheKey(me.getSourceSegmentId(), me.getSourceSegmentOffset());
        } else {
            // Return a CacheKey; if a RedirectIndexEntry then no data should be available in the cache.
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
        Exceptions.checkArgument(newMetadata.getLength() == this.metadata.getLength(), "newMetadata", "New Metadata Length is different from existing one.");
        Exceptions.checkArgument(newMetadata.getStorageLength() == this.metadata.getStorageLength(), "newMetadata", "New Metadata StorageLength is different from existing one.");
        Exceptions.checkArgument(newMetadata.isSealed() == this.metadata.isSealed(), "newMetadata", "New Metadata Sealed Flag is different from existing one.");
        Exceptions.checkArgument(newMetadata.isMerged() == this.metadata.isMerged(), "newMetadata", "New Metadata Merged Flag is different from existing one.");
        Exceptions.checkArgument(newMetadata.isDeleted() == this.metadata.isDeleted(), "newMetadata", "New Metadata Deletion Flag is different from existing one.");

        this.metadata = newMetadata;
        this.recoveryMode = false;
        log.info("{}: Exit RecoveryMode.", this.traceObjectId);
    }

    //endregion

    //region Index Updates

    /**
     * Appends the given range of bytes at the given offset.
     *
     * @param offset The offset within the StreamSegment to append at.
     * @param data   The range of bytes to append.
     * @throws NullPointerException     If data is null.
     * @throws IllegalArgumentException If the operation would cause writing beyond the StreamSegment's Length.
     * @throws IllegalArgumentException If the offset is invalid (does not match the previous append offset).
     */
    void append(long offset, byte[] data) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!isMerged(), "StreamSegment has been merged into a different one. Cannot append more ReadIndex entries.");

        if (data.length == 0) {
            // Nothing to do. Adding empty read entries will only make our system slower and harder to debug.
            return;
        }

        // Metadata check can be done outside the write lock.
        // Adding at the end means that we always need to "catch-up" with Length. Check to see if adding
        // this entry will make us catch up to it or not.
        long length = this.metadata.getLength();
        long endOffset = offset + data.length;
        Exceptions.checkArgument(endOffset <= length, "offset", "The given range of bytes (%d-%d) is beyond the StreamSegment Length (%d).", offset, endOffset, length);

        // Then append an entry for it in the ReadIndex. It's ok to insert into the cache outside of the lock here,
        // since there is no chance of competing with another write request for the same offset at the same time.
        this.cache.insert(new CacheKey(this.metadata.getId(), offset), data);
        appendEntry(new CacheIndexEntry(offset, data.length));
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
     * @throws IllegalArgumentException If the operation would cause writing beyond the StreamSegment's Length.
     * @throws IllegalArgumentException If the offset is invalid (does not match the previous append offset).
     * @throws IllegalArgumentException If sourceStreamSegmentIndex refers to a StreamSegment that is already merged.
     * @throws IllegalArgumentException If sourceStreamSegmentIndex refers to a StreamSegment that has a different parent
     *                                  StreamSegment than the current index's one.
     */
    void beginMerge(long offset, StreamSegmentReadIndex sourceStreamSegmentIndex) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "beginMerge", offset, sourceStreamSegmentIndex.traceObjectId);
        Exceptions.checkNotClosed(this.closed, this);
        Exceptions.checkArgument(!sourceStreamSegmentIndex.isMerged(), "sourceStreamSegmentIndex", "Given StreamSegmentReadIndex is already merged.");

        SegmentMetadata sourceMetadata = sourceStreamSegmentIndex.metadata;
        Exceptions.checkArgument(sourceMetadata.isSealed(), "sourceStreamSegmentIndex", "Given StreamSegmentReadIndex refers to a StreamSegment that is not sealed.");

        long sourceLength = sourceStreamSegmentIndex.getSegmentLength();
        RedirectIndexEntry newEntry = new RedirectIndexEntry(offset, sourceStreamSegmentIndex);
        if (sourceLength == 0) {
            // Nothing to do. Just record that there is a merge for this source Segment id.
            return;
        }

        // Metadata check can be done outside the write lock.
        // Adding at the end means that we always need to "catch-up" with Length. Check to see if adding
        // this entry will make us catch up to it or not.
        long ourLength = getSegmentLength();
        long endOffset = offset + sourceLength;
        Exceptions.checkArgument(endOffset <= ourLength, "offset", "The given range of bytes(%d-%d) is beyond the StreamSegment Length (%d).", offset, endOffset, ourLength);

        // Check and record the merger (optimistically).
        synchronized (this.lock) {
            Exceptions.checkArgument(!this.pendingMergers.containsKey(sourceMetadata.getId()), "sourceStreamSegmentIndex", "Given StreamSegmentReadIndex is already merged or in the process of being merged into this one.");
            this.pendingMergers.put(sourceMetadata.getId(), new PendingMerge(newEntry.key()));
        }

        try {
            appendEntry(newEntry);
        } catch (Exception ex) {
            // If the merger failed, roll back the markers.
            synchronized (this.lock) {
                this.pendingMergers.remove(sourceMetadata.getId());
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
     * @param sourceMetadata The SegmentMetadata of the Segment that was merged into this one.
     */
    void completeMerge(SegmentMetadata sourceMetadata) {
        long traceId = LoggerHelpers.traceEnterWithContext(log, this.traceObjectId, "completeMerge", sourceMetadata.getId());
        Exceptions.checkNotClosed(this.closed, this);
        Exceptions.checkArgument(sourceMetadata.isDeleted(), "sourceSegmentStreamId",
                "Given StreamSegmentReadIndex refers to a StreamSegment that has not been deleted yet.");

        if (sourceMetadata.getLength() == 0) {
            // beginMerge() does not take any action when the source Segment is empty, so there is nothing for us to do either.
            return;
        }

        // Find the appropriate redirect entry.
        RedirectIndexEntry redirectEntry;
        PendingMerge pendingMerge;
        synchronized (this.lock) {
            pendingMerge = this.pendingMergers.getOrDefault(sourceMetadata.getId(), null);
            Exceptions.checkArgument(pendingMerge != null, "sourceSegmentStreamId",
                    "Given StreamSegmentReadIndex's merger with this one has not been initiated using beginMerge. Cannot finalize the merger.");

            // Get the RedirectIndexEntry. These types of entries are sticky in the cache and DO NOT contribute to the
            // cache Stats. They are already accounted for in the other Segment's ReadIndex.
            ReadIndexEntry indexEntry = this.indexEntries.get(pendingMerge.getMergeOffset());
            assert indexEntry != null && !indexEntry.isDataEntry() :
                    String.format("pendingMergers points to a ReadIndexEntry that does not exist or is of the wrong type. sourceStreamSegmentId = %d, offset = %d, treeEntry = %s.",
                            sourceMetadata.getId(), pendingMerge.getMergeOffset(), indexEntry);
            redirectEntry = (RedirectIndexEntry) indexEntry;
        }

        StreamSegmentReadIndex sourceIndex = redirectEntry.getRedirectReadIndex();

        // Get all the entries from the source index and append them here.
        List<MergedIndexEntry> sourceEntries = sourceIndex.getAllEntries(redirectEntry.getStreamSegmentOffset());

        synchronized (this.lock) {
            // Remove redirect entry (again, no need to update the Cache Stats, as this is a RedirectIndexEntry).
            this.indexEntries.remove(pendingMerge.getMergeOffset());
            this.pendingMergers.remove(sourceMetadata.getId());
            sourceEntries.forEach(this::addToIndex);
        }

        List<FutureReadResultEntry> pendingReads = pendingMerge.seal();
        if (pendingReads.size() > 0) {
            log.debug("{}: triggerFutureReads for Pending Merge (Count = {}, MergeOffset = {}, MergeLength = {}).",
                    this.traceObjectId, pendingReads.size(), pendingMerge.getMergeOffset(), sourceIndex.getSegmentLength());
            triggerFutureReads(pendingReads);
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "completeMerge", traceId);
    }

    private void appendEntry(ReadIndexEntry entry) {
        log.debug("{}: Append (Offset = {}, Length = {}).", this.traceObjectId, entry.getStreamSegmentOffset(), entry.getLength());

        long lastOffset = entry.getLastStreamSegmentOffset();
        synchronized (this.lock) {
            Exceptions.checkArgument(this.lastAppendedOffset < 0 || entry.getStreamSegmentOffset() == this.lastAppendedOffset + 1, "entry", "The given range of bytes (%d-%d) does not start right after the last appended range (%d).", entry.getStreamSegmentOffset(), lastOffset, this.lastAppendedOffset);

            // Finally, append the entry.
            // Key is Offset + Length -1 = Last Offset Of Entry. Value is entry itself. This makes searching easier.
            ReadIndexEntry oldEntry = addToIndex(entry);
            assert oldEntry == null : String.format("Added a new entry in the ReadIndex that overrode an existing element. New = %s, Old = %s.", entry, oldEntry);
            this.lastAppendedOffset = lastOffset;
        }
    }

    private void insert(long offset, ByteArraySegment data) {
        log.debug("{}: Insert (Offset = {}, Length = {}).", this.traceObjectId, offset, data.getLength());

        // There is a very small chance we might be adding data twice, if we get two concurrent requests that slipped past
        // the StorageReadManager. Fixing it would be complicated, so let's see if it poses any problems.
        CacheIndexEntry entry = new CacheIndexEntry(offset, data.getLength());
        long lastOffset = entry.getLastStreamSegmentOffset();
        Exceptions.checkArgument(lastOffset < this.metadata.getStorageLength(), "entry",
                "The given range of bytes (%d-%d) does not correspond to the StreamSegment range that is in Storage (%d).",
                entry.getStreamSegmentOffset(), lastOffset, this.metadata.getStorageLength());
        ReadIndexEntry oldEntry;
        synchronized (this.lock) {
            this.cache.insert(getCacheKey(entry), data);
            oldEntry = addToIndex(entry);
        }

        if (oldEntry != null) {
            log.warn("{}: Insert overrode existing entry (Offset = {}, OldLength = {}, NewLength = {}).",
                    this.traceObjectId, entry.getStreamSegmentOffset(), entry.getLength(), oldEntry.getLength());
        }
    }

    @GuardedBy("lock")
    private ReadIndexEntry addToIndex(ReadIndexEntry entry) {
        // Insert the new entry and figure out if an old entry was overwritten.
        ReadIndexEntry oldEntry = this.indexEntries.put(entry);
        if (entry.isDataEntry()) {
            if (entry instanceof MergedIndexEntry) {
                // This entry has already existed in the cache for a while; do not change its generation.
                this.summary.add(entry.getLength(), entry.getGeneration());
            } else {
                // Update the Stats with the entry's length, and set the entry's generation as well.
                int generation = this.summary.add(entry.getLength());
                entry.setGeneration(generation);
            }
        }

        if (oldEntry != null && oldEntry.isDataEntry()) {
            // Need to eject the old entry's data from the Cache Stats.
            this.summary.remove(oldEntry.getLength(), oldEntry.getGeneration());
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
    void triggerFutureReads() {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!this.recoveryMode, "StreamSegmentReadIndex is in Recovery Mode.");

        boolean sealed = this.metadata.isSealed();
        Collection<FutureReadResultEntry> futureReads;
        if (sealed) {
            // Get everything, even if some Future Reads are in the future - those will eventually return EndOfSegment.
            futureReads = this.futureReads.pollAll();
            log.debug("{}: triggerFutureReads (Count = {}, Offset = {}, Sealed = True).", this.traceObjectId, futureReads.size(), this.metadata.getLength());
        } else {
            // Get all eligible Future Reads which wait for data prior to the end offset.
            // Since we are not actually using this entry's data, there is no need to 'touch' it.
            ReadIndexEntry lastEntry;
            synchronized (this.lock) {
                lastEntry = this.indexEntries.getLast();
            }

            if (lastEntry == null) {
                // Nothing to do.
                return;
            }

            // Get only those up to the last offset of the last append.
            futureReads = this.futureReads.poll(lastEntry.getLastStreamSegmentOffset());
            log.debug("{}: triggerFutureReads (Count = {}, Offset = {}, Sealed = False).", this.traceObjectId, futureReads.size(), lastEntry.getLastStreamSegmentOffset());
        }

        triggerFutureReads(futureReads);
    }

    /**
     * Triggers all the Future Reads in the given collection.
     *
     * @param futureReads The Future Reads to trigger.
     */
    private void triggerFutureReads(Collection<FutureReadResultEntry> futureReads) {
        for (FutureReadResultEntry r : futureReads) {
            ReadResultEntry entry = getSingleReadResultEntry(r.getStreamSegmentOffset(), r.getRequestedReadLength());
            assert entry != null : "Serving a StorageReadResultEntry with a null result";
            assert !(entry instanceof FutureReadResultEntry) : "Serving a FutureReadResultEntry with another FutureReadResultEntry.";

            log.trace("{}: triggerFutureReads (Offset = {}, Type = {}).", this.traceObjectId, r.getStreamSegmentOffset(), entry.getType());
            if (entry.getType() == ReadResultEntryType.EndOfStreamSegment) {
                // We have attempted to read beyond the end of the stream. Fail the read request with the appropriate message.
                r.fail(new StreamSegmentSealedException(String.format("StreamSegment has been sealed at offset %d. There can be no more reads beyond this offset.", this.metadata.getLength())));
            } else {
                if (!entry.getContent().isDone()) {
                    // Normally, all Future Reads are served from Cache, since they reflect data that has just been appended.
                    // However, it's possible that after recovery, we get a read for some data that we do not have in the
                    // cache (but it's not a tail read) - this data exists in Storage but our StorageLength has not yet been
                    // updated. As such, the only solution we have is to return a FutureRead which will be satisfied when
                    // the Writer updates the StorageLength (and trigger future reads). In that scenario, entry we get
                    // will likely not be auto-fetched, so we need to request the content.
                    entry.requestContent(this.config.getStorageReadDefaultTimeout());
                }

                CompletableFuture<ReadResultEntryContents> entryContent = entry.getContent();
                entryContent.thenAccept(r::complete);
                Futures.exceptionListener(entryContent, r::fail);
            }
        }
    }

    /**
     * Reads a contiguous sequence of bytes of the given length starting at the given offset. Every byte in the range
     * must meet the following conditions:
     * <ul>
     * <li> It must exist in this segment. This excludes bytes from merged transactions and future reads.
     * <li> It must be part of data that is not yet committed to Storage (tail part) - as such, it must be fully in the cache.
     * </ul>
     * Note: This method will not cause cache statistics to be updated. As such, Cache entry generations will not be
     * updated for those entries that are touched.
     *
     * @param startOffset The offset in the StreamSegment where to start reading.
     * @param length      The number of bytes to read.
     * @return An InputStream containing the requested data, or null if all of the conditions of this read cannot be met.
     * @throws IllegalStateException    If the read index is in recovery mode.
     * @throws IllegalArgumentException If the parameters are invalid (offset, length or offset+length are not in the Segment's range).
     */
    InputStream readDirect(long startOffset, int length) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!this.recoveryMode, "StreamSegmentReadIndex is in Recovery Mode.");
        Preconditions.checkArgument(length >= 0, "length must be a non-negative number");
        Preconditions.checkArgument(startOffset >= this.metadata.getStorageLength(), "startOffset must refer to an offset beyond the Segment's StorageLength offset.");
        Preconditions.checkArgument(startOffset + length <= this.metadata.getLength(), "startOffset+length must be less than the length of the Segment.");
        Preconditions.checkArgument(startOffset >= Math.min(this.metadata.getStartOffset(), this.metadata.getStorageLength()),
                "startOffset is before the Segment's StartOffset.");

        // Get the first entry. This one is trickier because the requested start offset may not fall on an entry boundary.
        CompletableReadResultEntry nextEntry;
        synchronized (this.lock) {
            ReadIndexEntry indexEntry = this.indexEntries.getFloor(startOffset);
            if (indexEntry == null || startOffset > indexEntry.getLastStreamSegmentOffset() || !indexEntry.isDataEntry()) {
                // Data not available or data exist in a partially merged transaction.
                return null;
            } else {
                // Fetch data from the cache for the first entry, but do not update the cache hit stats.
                nextEntry = createMemoryRead(indexEntry, startOffset, length, false);
            }
        }

        // Collect the contents of congruent Index Entries into a list, as long as we still encounter data in the cache.
        // Since we know all entries should be in the cache and are contiguous, there is no need
        assert Futures.isSuccessful(nextEntry.getContent()) : "Found CacheReadResultEntry that is not completed yet: " + nextEntry;
        val entryContents = nextEntry.getContent().join();

        ArrayList<InputStream> contents = new ArrayList<>();
        contents.add(entryContents.getData());
        int readLength = entryContents.getLength();
        while (readLength < length) {
            // No need to search the index; from now on, we know each offset we are looking for is at the beginning of a cache entry.
            // Also, no need to acquire the lock there. The cache itself is thread safe, and if the entry we are about to fetch
            // has just been evicted, we'll just get null back and stop reading (which is acceptable).
            byte[] entryData = this.cache.get(new CacheKey(this.metadata.getId(), startOffset + readLength));
            if (entryData == null) {
                // Could not find the 'next' cache entry: this means the requested range is not fully cached.
                return null;
            }

            int entryReadLength = Math.min(entryData.length, length - readLength);
            assert entryReadLength > 0 : "about to have fetched zero bytes from a cache entry";
            contents.add(new ByteArrayInputStream(entryData, 0, entryReadLength));
            readLength += entryReadLength;
        }

        // Coalesce the results into a single InputStream and return the result.
        return new SequenceInputStream(Iterators.asEnumeration(contents.iterator()));
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
    ReadResult read(long startOffset, int maxLength, Duration timeout) {
        Exceptions.checkNotClosed(this.closed, this);
        Preconditions.checkState(!this.recoveryMode, "StreamSegmentReadIndex is in Recovery Mode.");
        Exceptions.checkArgument(startOffset >= 0, "startOffset", "startOffset must be a non-negative number.");
        Exceptions.checkArgument(maxLength >= 0, "maxLength", "maxLength must be a non-negative number.");

        // We only check if we exceeded the last offset of a Sealed Segment. If we attempted to read from a truncated offset
        // that will be handled by returning a Truncated ReadResultEntryType.
        Exceptions.checkArgument(checkReadAvailability(startOffset, true) != ReadAvailability.BeyondLastOffset,
                "startOffset", "StreamSegment is sealed and startOffset is beyond the last offset of the StreamSegment.");

        log.debug("{}: Read (Offset = {}, MaxLength = {}).", this.traceObjectId, startOffset, maxLength);
        return new StreamSegmentReadResult(startOffset, maxLength, this::getMultiReadResultEntry, this.traceObjectId);
    }

    /**
     * Determines the availability of reading at a particular offset, given the state of a segment.
     *
     * @param offset              The offset to check.
     * @param lastOffsetInclusive If true, it will consider the last offset of the segment as a valid offset, otherwise
     *                            it will only validate offsets before the last offset in the segment.
     * @return A ReadAvailability based on the Segment's current state and the given offset. This will return Available
     * unless the given offset is before the Segment's StartOffset or beyond its Length and the Segment is Sealed.
     */
    private ReadAvailability checkReadAvailability(long offset, boolean lastOffsetInclusive) {
        // We can only read at a particular offset if:
        // * The offset is not before the Segment's StartOffset
        // AND
        // * The segment is not sealed (we are allowed to do a future read) OR
        // * The segment is sealed and we are not trying to read at or beyond the last offset (based on input).
        if (offset < this.metadata.getStartOffset()) {
            return ReadAvailability.BeforeStartOffset;
        } else if (this.metadata.isSealed()) {
            return offset < (this.metadata.getLength() + (lastOffsetInclusive ? 1 : 0))
                    ? ReadAvailability.Available
                    : ReadAvailability.BeyondLastOffset;
        }

        // Offset is in a valid range.
        return ReadAvailability.Available;
    }

    /**
     * Returns the first ReadResultEntry that matches the specified search parameters.
     *
     * Compared to getMultiReadResultEntry(), this method returns exactly one ReadResultEntry.
     *
     * Compared to getSingleMemoryReadResultEntry(), this method will return a CompletableReadResultEntry regardless of
     * whether the data is cached or not. This may involve registering a future read or triggering a Storage read if necessary,
     * as well as redirecting the read to a Transaction if necessary.
     *
     * @param resultStartOffset The Offset within the StreamSegment where to start returning data from.
     * @param maxLength         The maximum number of bytes to return.
     * @return A ReadResultEntry representing the data to return.
     */
    private CompletableReadResultEntry getSingleReadResultEntry(long resultStartOffset, int maxLength) {
        Exceptions.checkNotClosed(this.closed, this);

        if (maxLength < 0) {
            // Nothing to read.
            return null;
        }

        CompletableReadResultEntry result = null;
        // Check to see if we are trying to read beyond the last offset of a sealed StreamSegment.
        ReadAvailability ra = checkReadAvailability(resultStartOffset, false);
        if (ra == ReadAvailability.BeyondLastOffset) {
            result = new EndOfStreamSegmentReadResultEntry(resultStartOffset, maxLength);
        } else if (ra == ReadAvailability.BeforeStartOffset) {
            result = new TruncatedReadResultEntry(resultStartOffset, maxLength, this.metadata.getStartOffset());
        } else {
            // Look up an entry in the index that contains our requested start offset.
            synchronized (this.lock) {
                ReadIndexEntry indexEntry = this.indexEntries.getFloor(resultStartOffset);
                if (indexEntry == null) {
                    // No data in the index or we have at least one entry and the ResultStartOffset is before the Start Offset
                    // of the first entry in the index. Use the metadata to figure out whether to return a Storage or Future Read.
                    result = createDataNotAvailableRead(resultStartOffset, maxLength);
                } else {
                    // We have an entry. Let's see if it's valid or not.
                    if (resultStartOffset > indexEntry.getLastStreamSegmentOffset()) {
                        // ResultStartOffset is beyond the End Offset of this entry. This means either:
                        // 1. This is the last entry and ResultStartOffset is after it. OR
                        // 2. We have a gap in our entries, and ResultStartOffset is somewhere in there.
                        result = createDataNotAvailableRead(resultStartOffset, maxLength);
                    } else if (indexEntry.isDataEntry()) {
                        // ResultStartOffset is after the StartOffset and before the End Offset of this entry.
                        result = createMemoryRead(indexEntry, resultStartOffset, maxLength, true);
                    } else if (indexEntry instanceof RedirectIndexEntry) {
                        // ResultStartOffset is after the StartOffset and before the End Offset of this entry, but this
                        // is a Redirect; reissue the request to the appropriate index.
                        result = createRedirectedRead(resultStartOffset, maxLength, (RedirectIndexEntry) indexEntry);
                    }
                }
            }
        }

        // Just before exiting, check we are returning something. We should always return something if we coded this correctly.
        assert result != null : String.format("Reached the end of getSingleReadResultEntry(id=%d, offset=%d, length=%d) "
                + "with no plausible result in sight. This means we missed a case.", this.metadata.getId(), resultStartOffset, maxLength);
        return result;
    }

    /**
     * Returns a ReadResultEntry that matches the specified search parameters.
     * <p>
     * Compared to getSingleMemoryReadResultEntry(), this method may return a direct entry or a collection of entries.
     * If the first entry to be returned would constitute a cache hit, then this method will attempt to return data from
     * subsequent (congruent) entries, as long as they are cache hits. If at any time a cache miss occurs, the data collected
     * so far is returned as a single entry, excluding the cache miss entry (exception if the first entry is a miss,
     * then that entry is returned).
     *
     * @param resultStartOffset The Offset within the StreamSegment where to start returning data from.
     * @param maxLength         The maximum number of bytes to return.
     * @return A ReadResultEntry representing the data to return.
     */
    private CompletableReadResultEntry getMultiReadResultEntry(long resultStartOffset, int maxLength) {
        int readLength = 0;

        CompletableReadResultEntry nextEntry = getSingleReadResultEntry(resultStartOffset, maxLength);
        if (nextEntry == null || !(nextEntry instanceof CacheReadResultEntry)) {
            // We can only coalesce CacheReadResultEntries.
            return nextEntry;
        }

        // Collect the contents of congruent Index Entries into a list, as long as we still encounter data in the cache.
        ArrayList<InputStream> contents = new ArrayList<>();
        do {
            assert Futures.isSuccessful(nextEntry.getContent()) : "Found CacheReadResultEntry that is not completed yet: " + nextEntry;
            val entryContents = nextEntry.getContent().join();
            contents.add(entryContents.getData());
            readLength += entryContents.getLength();
            if (readLength >= this.config.getMemoryReadMinLength() || readLength >= maxLength) {
                break;
            }

            nextEntry = getSingleMemoryReadResultEntry(resultStartOffset + readLength, maxLength - readLength);
        } while (nextEntry != null);

        // Coalesce the results into a single InputStream and return the result.
        return new CacheReadResultEntry(resultStartOffset, new SequenceInputStream(Iterators.asEnumeration(contents.iterator())), readLength);
    }

    /**
     * Returns a CacheReadResultEntry that matches the specified search parameters, but only if the data is readily available
     * in the cache and if there is an index entry that starts at that location.. As opposed from getSingleReadResultEntry(),
     * this method will return null if the requested data is not available.
     *
     * @param resultStartOffset The Offset within the StreamSegment where to start returning data from.
     * @param maxLength         The maximum number of bytes to return.
     * @return A CacheReadResultEntry representing the data to return.
     */
    private CacheReadResultEntry getSingleMemoryReadResultEntry(long resultStartOffset, int maxLength) {
        Exceptions.checkNotClosed(this.closed, this);

        if (maxLength > 0 && checkReadAvailability(resultStartOffset, false) == ReadAvailability.Available) {
            // Look up an entry in the index that contains our requested start offset.
            synchronized (this.lock) {
                ReadIndexEntry indexEntry = this.indexEntries.get(resultStartOffset);
                if (indexEntry != null && indexEntry.isDataEntry()) {
                    // We found an entry; return a result for it.
                    return createMemoryRead(indexEntry, resultStartOffset, maxLength, true);
                }
            }
        }

        // Nothing could be found in the cache at the given offset.
        return null;
    }

    private CompletableReadResultEntry createRedirectedRead(long streamSegmentOffset, int maxLength, RedirectIndexEntry entry) {
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

        CompletableReadResultEntry result = redirectedIndex.getSingleReadResultEntry(redirectOffset, maxLength);
        if (result != null) {
            // Since this is a redirect to a (merged) Transaction, it is possible that between now and when the caller
            // invokes the requestContent() on the entry the Transaction may be fully merged (in Tier2). If that's the
            // case, then this entry will fail with either ObjectClosedException or StreamSegmentNotFoundException, since
            // it is pointing to the now defunct Transaction segment. At that time, a simple retry of the read would
            // yield the right result. However, in order to recover from this without the caller's intervention, we pass
            // a pointer to getSingleReadResultEntry to the RedirectedReadResultEntry in case it fails with such an exception;
            // that class has logic in it to invoke it if needed and get the right entry.
            result = new RedirectedReadResultEntry(result, entry.getStreamSegmentOffset(), this::getOrRegisterRedirectedRead, redirectedIndex.metadata.getId());
        }

        return result;
    }

    private CompletableReadResultEntry getOrRegisterRedirectedRead(long resultStartOffset, int maxLength, long sourceSegmentId) {
        CompletableReadResultEntry result = getSingleReadResultEntry(resultStartOffset, maxLength);
        if (result instanceof RedirectedReadResultEntry) {
            // The merger isn't completed yet. Register the read so that it is completed when the merger is done.
            PendingMerge pendingMerge;
            synchronized (this.lock) {
                pendingMerge = this.pendingMergers.getOrDefault(sourceSegmentId, null);
            }

            // Transform the read result entry into a Future Read, instead of adding it to futureReads, associate it
            // with this pendingMerge, so that a completeMerge() would finalize it.
            FutureReadResultEntry futureResult = new FutureReadResultEntry(result.getStreamSegmentOffset(), result.getRequestedReadLength());
            if (pendingMerge != null && pendingMerge.register(futureResult)) {
                // We were able to register the result.
                result = futureResult;
                log.debug("{}: Registered Pending Merge Future Read {}.", this.traceObjectId, result);
            } else {
                // The merge has been unregistered. Our only hope now is that the index has settled and re-invoking this
                // will yield the sought-after result.
                if (pendingMerge == null) {
                    log.debug("{}: Could not find Pending Merge for Id {} for {}; re-issuing.", this.traceObjectId, sourceSegmentId, result);
                } else {
                    log.debug("{}: Pending Merge for id {} was sealed for {}; re-issuing.", this.traceObjectId, sourceSegmentId, result);
                }

                result = getSingleReadResultEntry(resultStartOffset, maxLength);
            }
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
        maxLength = getLengthUntilNextEntry(streamSegmentOffset, maxLength);
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
            // Note that Future Reads are not necessarily tail reads. They mean that we cannot return a result given
            // the current state of the metadata. An example of when we might return a Future Read that is not a tail read
            // is when we receive a read request immediately after recovery, but before the StorageWriter has had a chance
            // to refresh the Storage state (the metadata may be a bit out of date). In that case, we record a Future Read
            // which will be completed when the StorageWriter invokes triggerFutureReads() upon refreshing the info.
            return createFutureRead(streamSegmentOffset, maxLength);
        }
    }

    /**
     * Creates a ReadResultEntry for data that is readily available in memory.
     *
     * @param entry               The CacheIndexEntry to use.
     * @param streamSegmentOffset The Offset in the StreamSegment where to the ReadResultEntry starts at.
     * @param maxLength           The maximum length of the Read, from the Offset of this ReadResultEntry.
     * @param updateStats         If true, the entry's cache generation is updated as a result of this call.
     */
    @GuardedBy("lock")
    private CacheReadResultEntry createMemoryRead(ReadIndexEntry entry, long streamSegmentOffset, int maxLength, boolean updateStats) {
        assert streamSegmentOffset >= entry.getStreamSegmentOffset() : String.format("streamSegmentOffset{%d} < entry.getStreamSegmentOffset{%d}", streamSegmentOffset, entry.getStreamSegmentOffset());

        int entryOffset = (int) (streamSegmentOffset - entry.getStreamSegmentOffset());

        int length = (int) Math.min(maxLength, entry.getLength() - entryOffset);
        assert length > 0 : String.format("length{%d} <= 0. streamSegmentOffset = %d, maxLength = %d, entry.offset = %d, entry.length = %d", length, streamSegmentOffset, maxLength, entry.getStreamSegmentOffset(), entry.getLength());
        byte[] data = this.cache.get(getCacheKey(entry));
        assert data != null : String.format("No Cache Entry could be retrieved for entry %s", entry);

        if (updateStats) {
            // Update its generation before returning it.
            int generation = this.summary.touchOne(entry.getGeneration());
            entry.setGeneration(generation);
        }

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
        Consumer<StorageReadManager.Result> doneCallback = result -> {
            ByteArraySegment data = result.getData();

            // Make sure we invoke our callback first, before any chance of exceptions from insert() may block it.
            successCallback.accept(new ReadResultEntryContents(data.getReader(), data.getLength()));
            if (!result.isDerived()) {
                // Only insert primary results into the cache. Derived results are always sub-portions of primaries
                // and there is no need to insert them too, as they are already contained within.
                insert(offset, data);
            }
        };

        // Queue the request for async processing.
        length = getReadAlignedLength(offset, length);
        this.storageReadManager.execute(new StorageReadManager.Request(offset, length, doneCallback, failureCallback, timeout));
    }

    /**
     * Returns the length from the given offset until the beginning of the next index entry. If no such entry exists, or
     * if the length is greater than maxLength, then maxLength is returned.
     *
     * @param startOffset The offset to search from.
     * @param maxLength   The maximum allowed length.
     * @return The result.
     */
    @GuardedBy("lock")
    private int getLengthUntilNextEntry(long startOffset, int maxLength) {
        ReadIndexEntry ceilingEntry = this.indexEntries.getCeiling(startOffset);
        if (ceilingEntry != null) {
            maxLength = (int) Math.min(maxLength, ceilingEntry.getStreamSegmentOffset() - startOffset);
        }

        return maxLength;
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
        int lengthSinceLastMultiple = (int) (offset % this.config.getStorageReadAlignment());

        // Even though we were asked to read a number of bytes, in some cases we will return fewer bytes than requested
        // in order to read-align the reads.
        return Math.min(readLength, this.config.getStorageReadAlignment() - lengthSinceLastMultiple);
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
    private List<MergedIndexEntry> getAllEntries(long offsetAdjustment) {
        Exceptions.checkArgument(offsetAdjustment >= 0, "offsetAdjustment", "offsetAdjustment must be a non-negative number.");

        synchronized (this.lock) {
            List<MergedIndexEntry> result = new ArrayList<>(this.indexEntries.size());
            this.indexEntries.forEach(entry -> {
                if (entry.isDataEntry()) {
                    result.add(new MergedIndexEntry(entry.getStreamSegmentOffset() + offsetAdjustment, this.metadata.getId(), (CacheIndexEntry) entry));
                }
            });

            return result;
        }
    }

    //endregion

    //region ReadAvailability

    private enum ReadAvailability {
        /**
         * The current position is OK for reading.
         */
        Available,

        /**
         * The current position is beyond the last readable offset of the Segment.
         */
        BeyondLastOffset,

        /**
         * The current position is before the first readable offset of the Segment.
         */
        BeforeStartOffset
    }

    //endregion
}