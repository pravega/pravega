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

package com.emc.logservice.server.reading;

import com.emc.logservice.common.AutoReleaseLock;
import com.emc.nautilus.common.Exceptions;
import com.emc.nautilus.common.LoggerHelpers;
import com.emc.logservice.common.ReadWriteAutoReleaseLock;
import com.emc.logservice.contracts.ReadResult;
import com.emc.logservice.contracts.ReadResultEntry;
import com.emc.logservice.contracts.ReadResultEntryType;
import com.emc.logservice.contracts.StreamSegmentSealedException;
import com.emc.logservice.server.ContainerMetadata;
import com.emc.logservice.server.SegmentMetadata;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
class StreamSegmentReadIndex implements AutoCloseable {
    //region Members

    private final String traceObjectId;
    private final TreeMap<Long, ReadIndexEntry> entries; // Key = Last Offset of Entry, Value = Entry; TODO: we can implement a version of this that doesn't require Key
    private final FutureReadResultEntryCollection futureReads;
    private final ReadWriteAutoReleaseLock lock;
    private final HashMap<Long, Long> mergeOffsets; //Key = StreamSegmentId (Merged), Value = Merge offset.
    private SegmentMetadata metadata;
    private long lastAppendedOffset;
    private boolean recoveryMode;
    private boolean closed;
    private boolean merged;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentReadIndex class.
     *
     * @param metadata The StreamSegmentMetadata to use.
     */
    protected StreamSegmentReadIndex(SegmentMetadata metadata, boolean recoveryMode, String containerId) {
        Preconditions.checkNotNull(metadata, "metadata");

        this.traceObjectId = String.format("ReadIndex[%s-%d]", containerId, metadata.getId());
        this.metadata = metadata;
        this.recoveryMode = recoveryMode;
        this.entries = new TreeMap<>();
        this.futureReads = new FutureReadResultEntryCollection();
        this.lock = new ReadWriteAutoReleaseLock(true);
        this.mergeOffsets = new HashMap<>();
        this.lastAppendedOffset = -1;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;

            // Cancel future reads.
            this.futureReads.close();
            log.info("{}: Closed.", this.traceObjectId);
        }
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating whether this Read Index is merged into another one.
     *
     * @return
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
        appendEntry(new ByteArrayReadIndexEntry(offset, data));
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
     * @throws IllegalArgumentException If sourceStreamSegmentIndex refers to a StreamSegment that is already merged..
     * @throws IllegalArgumentException If sourceStreamSegmentIndex refers to a StreamSegment that has a different parent
     *                                  StreamSegment than the current index's one.
     */
    public void beginMerge(long offset, StreamSegmentReadIndex sourceStreamSegmentIndex) {
        int traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "beginMerge", offset, sourceStreamSegmentIndex.traceObjectId);
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
        try (AutoReleaseLock ignored = lock.acquireWriteLock()) {
            Exceptions.checkArgument(!this.mergeOffsets.containsKey(sourceMetadata.getId()), "sourceStreamSegmentIndex", "Given StreamSegmentReadIndex is already merged or in the process of being merged into this one.");
            this.mergeOffsets.put(sourceMetadata.getId(), newEntry.getLastStreamSegmentOffset());
        }

        try {
            appendEntry(newEntry);
        } catch (Exception ex) {
            // If the merger failed, roll back the markers.
            try (AutoReleaseLock ignored = lock.acquireWriteLock()) {
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
     * @param sourceSegmentStreamId
     */
    public void completeMerge(long sourceSegmentStreamId) {
        int traceId = LoggerHelpers.traceEnter(log, this.traceObjectId, "completeMerge", sourceSegmentStreamId);
        Exceptions.checkNotClosed(this.closed, this);

        // Find the appropriate redirect entry.
        RedirectReadIndexEntry redirectEntry;
        long endOffset;
        try (AutoReleaseLock ignored = lock.acquireReadLock()) {
            endOffset = this.mergeOffsets.getOrDefault(sourceSegmentStreamId, -1L);
            Exceptions.checkArgument(endOffset >= 0, "sourceSegmentStreamId", "Given StreamSegmentReadIndex's merger with this one has not been initiated using beginMerge. Cannot finalize the merger.");

            ReadIndexEntry treeEntry = this.entries.getOrDefault(endOffset, null);
            assert treeEntry != null && (treeEntry instanceof RedirectReadIndexEntry) : String.format("mergeOffsets points to a ReadIndexEntry that does not exist or is of the wrong type. sourceStreamSegmentId = %d, offset = %d, treeEntry = %s.", sourceSegmentStreamId, endOffset, treeEntry);
            redirectEntry = (RedirectReadIndexEntry) treeEntry;
        }

        StreamSegmentReadIndex sourceIndex = redirectEntry.getRedirectReadIndex();
        SegmentMetadata sourceMetadata = sourceIndex.metadata;
        Exceptions.checkArgument(sourceMetadata.isDeleted(), "sourceSegmentStreamId", "Given StreamSegmentReadIndex refers to a StreamSegment that has not been deleted yet.");

        // TODO: an alternative to this is just drop the RedirectReadIndexEntry; next time we want to read, we'll just read from storage. That may be faster actually than just appending all these entries (there could be tens of thousands...)
        // Get all the entries from the source index and append them here. TODO: should we coalesce them too (into bigger entries)?
        List<ByteArrayReadIndexEntry> sourceEntries = sourceIndex.getAllEntries(redirectEntry.getStreamSegmentOffset());

        try (AutoReleaseLock ignored = lock.acquireWriteLock()) {
            // Remove redirect entry.
            this.entries.remove(endOffset);
            this.mergeOffsets.remove(sourceSegmentStreamId);

            // TODO: Verify offsets are correct and that they do not exceed boundaries.
            for (ByteArrayReadIndexEntry e : sourceEntries) {
                this.entries.put(e.getLastStreamSegmentOffset(), e);
            }
        }

        LoggerHelpers.traceLeave(log, this.traceObjectId, "completeMerge", traceId);
    }

    private void appendEntry(ReadIndexEntry entry) {
        log.debug("{}: Append (Offset = {}, Length = {}).", this.traceObjectId, entry.getStreamSegmentOffset(), entry.getLength());

        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            Exceptions.checkArgument(this.lastAppendedOffset < 0 || entry.getStreamSegmentOffset() == this.lastAppendedOffset + 1, "entry", "The given range of bytes (%d-%d) does not start right after the last appended range (%d).", entry.getStreamSegmentOffset(), entry.getLastStreamSegmentOffset(), this.lastAppendedOffset);

            // Finally, append the entry.
            // Key is Offset + Length -1 = Last Offset Of Entry. Value is entry itself. This makes searching easier.
            ReadIndexEntry oldEntry = this.entries.put(entry.getLastStreamSegmentOffset(), entry);
            assert oldEntry == null : String.format("Added a new entry in the ReadIndex that overrode an existing element. New = %s, Old = %s.", entry, oldEntry);
            this.lastAppendedOffset = entry.getLastStreamSegmentOffset();
        }
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

        if (this.entries.size() == 0) {
            // Nothing to do.
            return;
        }

        // Get all eligible Future Reads which wait for data prior to the end offset.
        ReadIndexEntry lastEntry = this.entries.lastEntry().getValue(); // NOTE: this is O(log(n)), not O(1)
        Collection<FutureReadResultEntry> futureReads;
        boolean sealed = this.metadata.isSealed();
        if (sealed) {
            // Get everything, even if some Future Reads are in the future - those will eventually return EndOfSegment.
            futureReads = this.futureReads.pollAll();
        } else {
            // Get only those up to the last offset of the last append.
            futureReads = this.futureReads.poll(lastEntry.getLastStreamSegmentOffset());
        }

        log.debug("{}: triggerFutureReads (Count = {}, Sealed = {}).", this.traceObjectId, futureReads.size(), sealed);

        for (FutureReadResultEntry r : futureReads) {
            ReadResultEntry entry = getFirstReadResultEntry(r.getStreamSegmentOffset(), r.getRequestedReadLength());
            assert entry != null : "Serving a StorageReadResultEntry with a null result";
            assert !(entry instanceof StorageReadResultEntry) : "Serving a StorageReadResultEntry with another StorageReadResultEntry.";

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
    private ReadResultEntryBase getFirstReadResultEntry(long resultStartOffset, int maxLength) {
        Exceptions.checkNotClosed(this.closed, this);

        if (maxLength < 0) {
            // Nothing to read.
            return null;
        }

        // Check to see if we are trying to read beyond the last offset of a sealed StreamSegment.
        if (!canReadAtOffset(resultStartOffset)) {
            return new EndOfStreamSegmentReadResultEntry(resultStartOffset, maxLength);
        }

        ReadResultEntryBase result = null;
        try (AutoReleaseLock ignored = this.lock.acquireReadLock()) {
            if (this.entries.size() == 0) {
                // We have no entries in the Read Index.
                // Use the metadata to figure out whether to return a Storage or Future Read.
                result = createDataNotAvailableRead(resultStartOffset, maxLength);
            } else {
                // We have at least one entry.
                // Find the first entry that has an End offset beyond equal to at least ResultStartOffset.
                Map.Entry<Long, ReadIndexEntry> treeEntry = this.entries.ceilingEntry(resultStartOffset);
                if (treeEntry == null) {
                    // The ResultStartOffset is beyond the End Offset of the last entry in the index.
                    // Use the metadata to figure out whether to return a Storage or Future Read, since we do not have
                    // this data in memory.
                    result = createDataNotAvailableRead(resultStartOffset, maxLength);
                } else {
                    // We have an entry. Let's see if it's valid or not.
                    ReadIndexEntry currentEntry = treeEntry.getValue();
                    if (resultStartOffset < currentEntry.getStreamSegmentOffset()) {
                        // ResultStartOffset is before the Start Offset of this entry. This means either:
                        // 1. This is the first entry and ResultStartOffset is before it. OR
                        // 2. We have a gap in our entries, and ResultStartOffset is somewhere in there.
                        // We must issue a Storage Read to bring the data to us (with a readLength of up to the size of the gap).
                        int readLength = (int) Math.min(maxLength, currentEntry.getStreamSegmentOffset() - resultStartOffset);
                        result = createStorageRead(resultStartOffset, readLength);
                    } else if (currentEntry instanceof ByteArrayReadIndexEntry) {
                        // ResultStartOffset is after the StartOffset and before the End Offset of this entry.
                        result = createMemoryRead((ByteArrayReadIndexEntry) currentEntry, resultStartOffset, maxLength);
                    } else if (currentEntry instanceof RedirectReadIndexEntry) {
                        result = getRedirectedReadResultEntry(resultStartOffset, maxLength, (RedirectReadIndexEntry) currentEntry);
                    }
                }
            }
        }

        if (result != null) {
            return result;
        } else {
            // We should never get in here if we coded this correctly.
            throw new AssertionError(String.format("Reached the end of getFirstReadResultEntry(id=%d, offset=%d, length=%d) with no plausible result in sight. This means we missed a case.", this.metadata.getId(), resultStartOffset, maxLength));
        }
    }

    private ReadResultEntryBase getRedirectedReadResultEntry(long streamSegmentOffset, int maxLength, RedirectReadIndexEntry entry) {
        StreamSegmentReadIndex redirectedIndex = entry.getRedirectReadIndex();
        long redirectOffset = streamSegmentOffset - entry.getStreamSegmentOffset();
        assert redirectOffset >= 0 && redirectOffset < entry.getLength() :
                String.format("Redirected offset would be outside of the range of the Redirected StreamSegment. StreamSegmentOffset = %d, MaxLength = %d, Entry.StartOffset = %d, Entry.Length = %d, RedirectOffset = %d.",
                        streamSegmentOffset,
                        maxLength,
                        entry.getStreamSegmentOffset(),
                        entry.getLength(),
                        redirectOffset);

        if (entry.getLength() < maxLength) {
            maxLength = (int) entry.getLength();
        }

        ReadResultEntryBase result = redirectedIndex.getFirstReadResultEntry(redirectOffset, maxLength);
        result.adjustOffset(entry.getStreamSegmentOffset());
        return result;
    }

    /**
     * Creates a ReadResultEntry that is a placeholder for data that is not currently available in memory.
     *
     * @param streamSegmentOffset
     * @param maxLength
     * @return
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
     * @param entry
     * @param streamSegmentOffset
     * @param maxLength
     * @return
     */
    private ReadResultEntryBase createMemoryRead(ByteArrayReadIndexEntry entry, long streamSegmentOffset, int maxLength) {
        assert streamSegmentOffset >= entry.getStreamSegmentOffset() : String.format("streamSegmentOffset{%d} < entry.getStreamSegmentOffset{%d}", streamSegmentOffset, entry.getStreamSegmentOffset());

        int entryOffset = (int) (streamSegmentOffset - entry.getStreamSegmentOffset());
        int length = Math.min(maxLength, entry.getData().length - entryOffset);
        assert length > 0 : String.format("length{%d} <= 0. streamSegmentOffset = %d, maxLength = %d, entry.offset = %d, entry.length = %d", length, streamSegmentOffset, maxLength, entry.getStreamSegmentOffset(), entry.getData().length);

        return new MemoryReadResultEntry(entry, entryOffset, length);
    }

    /**
     * Creates a ReadResultEntry that is a placeholder for data that is not in memory, but exists in Storage.
     *
     * @param streamSegmentOffset
     * @param readLength
     * @return
     */
    private ReadResultEntryBase createStorageRead(long streamSegmentOffset, int readLength) {
        // TODO: implement Storage Reads.
        return new StorageReadResultEntry(streamSegmentOffset, readLength, null);
    }

    /**
     * Creates a ReadResultEntry that is a placeholder for data that is not in memory, or in storage, which has
     * a starting offset beyond the length of the StreamSegment.
     *
     * @param streamSegmentOffset
     * @param maxLength
     * @return
     */
    private ReadResultEntryBase createFutureRead(long streamSegmentOffset, int maxLength) {
        FutureReadResultEntry entry = new FutureReadResultEntry(streamSegmentOffset, maxLength);
        this.futureReads.add(entry);
        return entry;
    }

    /**
     * Gets a copy of all the ReadIndexEntries in this Index that are of type ByteArrayReadIndexEntry. All returned
     * entries have their offsets adjusted by the given amount.
     *
     * @param offsetAdjustment The amount to adjust the offset by.
     * @return
     */
    private List<ByteArrayReadIndexEntry> getAllEntries(long offsetAdjustment) {
        Exceptions.checkArgument(offsetAdjustment >= 0, "offsetAdjustment", "offsetAdjustment must be a non-negative number.");

        try (AutoReleaseLock ignored = this.lock.acquireReadLock()) {
            ArrayList<ByteArrayReadIndexEntry> result = new ArrayList<>(this.entries.size());
            for (ReadIndexEntry entry : this.entries.values()) {
                if (!(entry instanceof ByteArrayReadIndexEntry)) {
                    continue;
                }

                result.add(new ByteArrayReadIndexEntry(entry.getStreamSegmentOffset() + offsetAdjustment, ((ByteArrayReadIndexEntry) entry).getData()));
            }

            return result;
        }
    }
    //endregion
}

