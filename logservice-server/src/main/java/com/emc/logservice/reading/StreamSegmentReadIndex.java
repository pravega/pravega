package com.emc.logservice.reading;

import com.emc.logservice.*;
import com.emc.logservice.core.*;

import java.time.Duration;
import java.util.*;

/**
 * Read Index for a single StreamSegment. Integrates reading data from the following sources:
 * <ol>
 * <li> The tail-end part of the StreamSegment (the part that is in DurableLog, but not yet in Storage).
 * <li> The part of the StreamSegment that is in Storage, but not in DurableLog. This data will be brought into memory
 * for fast read-ahead access.
 * <li> Future appends. If a read operation requests data from an offset in the future, the read operation will block until
 * data becomes available or until it gets canceled.
 * </ol>
 * TODO: Implement bringing data from Storage into the Read Index (in an elegant manner).
 * TODO: Uses a Cache Retention Policy to determine which data can be kept in memory and which can be evicted.
 */
class StreamSegmentReadIndex implements AutoCloseable {
    //region Members

    private SegmentMetadata metadata;
    private final TreeMap<Long, ReadIndexEntry> entries; // Key = Last Offset of Entry, Value = Entry; TODO: we can implement a version of this that doesn't require Key
    private final PlaceholderReadResultEntryCollection futureReads;
    private final ReadWriteAutoReleaseLock lock;
    private final HashMap<Long, Long> mergeOffsets; //Key = StreamSegmentId (Merged), Value = Merge offset.
    private long lastAppendedOffset;
    private boolean recoveryMode;
    private boolean closed;
    private boolean merged;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreameSegmentReadIndex class.
     *
     * @param metadata The StreamSegmentMetadata to use.
     */
    protected StreamSegmentReadIndex(SegmentMetadata metadata, boolean recoveryMode) {
        if (metadata == null) {
            throw new NullPointerException("metaData");
        }

        this.metadata = metadata;
        this.recoveryMode = recoveryMode;
        this.entries = new TreeMap<>();
        this.futureReads = new PlaceholderReadResultEntryCollection();
        this.lock = new ReadWriteAutoReleaseLock(true);
        this.mergeOffsets = new HashMap<>();
        this.lastAppendedOffset = -1;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        this.closed = true;

        // Cancel future reads.
        this.futureReads.close();

        // We do not close merged StreamSegmentReadIndices; that's the responsibility of the owning ReadIndex.
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
        if (this.merged) {
            throw new IllegalStateException("StreamSegmentReadIndexEntry is already merged.");
        }
        this.merged = true;
    }

    //endregion

    //region Recovery

    /**
     * Puts this Read Index out of recovery mode, while pointing it to the new metadata (Metadata objects can differ
     * between recovery and non-recovery).
     *
     * @param newMetadata The new metadata object to use from now on.
     * @throws ObjectClosedException    If this object has been closed.
     * @throws IllegalStateException    If the Read Index is not in recovery mode.
     * @throws NullPointerException     If the given metadata is null.
     * @throws IllegalArgumentException If the new metadata does not match the old one perfectly.
     */
    public void exitRecoveryMode(SegmentMetadata newMetadata) {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        if (!this.recoveryMode) {
            throw new IllegalStateException("Read Index is not in recovery mode.");
        }

        if (newMetadata == null) {
            throw new NullPointerException("newMetadata");
        }

        if (newMetadata.getId() != this.metadata.getId()) {
            throw new IllegalArgumentException("New Metadata StreamSegmentId is different from existing one.");
        }

        if (newMetadata.getDurableLogLength() != this.metadata.getDurableLogLength()) {
            throw new IllegalArgumentException("New Metadata DurableLogLength is different from existing one.");
        }

        if (newMetadata.getStorageLength() != this.metadata.getStorageLength()) {
            throw new IllegalArgumentException("New Metadata StorageLength is different from existing one.");
        }

        if (newMetadata.isSealed() != this.metadata.isSealed()) {
            throw new IllegalArgumentException("New Metadata Sealed Flag is different from existing one.");
        }

        if (newMetadata.isDeleted() != this.metadata.isDeleted()) {
            throw new IllegalArgumentException("New Metadata Deletion Flag is different from existing one.");
        }

        this.metadata = newMetadata;
        this.recoveryMode = false;
    }

    //endregion

    //region Appending

    /**
     * Appends the given range of bytes at the given offset.
     *
     * @param offset The offset within the StreamSegment to add at.
     * @param data   The range of bytes to add.
     * @throws ObjectClosedException    If the StreamSegmentReadIndex is closed.
     * @throws NullPointerException     If data is null.
     * @throws IllegalArgumentException If the operation would cause writing beyond the StreamSegment's DurableLogLength.
     * @throws IllegalArgumentException If the offset is invalid (does not match the previous add offset).
     */
    public void append(long offset, byte[] data) {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        if (isMerged()) {
            throw new IllegalStateException("StreamSegment has been merged into a different one. Cannot add more cache entries.");
        }

        if (data.length == 0) {
            // Nothing to do. Adding empty read entries will only make our system slower and harder to debug.
            return;
        }

        // Metadata check can be done outside the write lock.
        // Adding at the end means that we always need to "catch-up" with DurableLogLength. Check to see if adding
        // this entry will make us catch up to it or not.
        long durableLogLength = this.metadata.getDurableLogLength();
        long endOffset = offset + data.length;
        if (endOffset > durableLogLength) {
            throw new IllegalArgumentException(String.format("The given range of bytes (%d-%d) is beyond the StreamSegment Durable Log Length (%d).", offset, endOffset, durableLogLength));
        }

        append(new ByteArrayReadIndexEntry(offset, data));
    }

    /**
     * Executes Step 1 of the 2-Step Merge Process.
     * The StreamSegments are merged (Source->Target@Offset) in Metadata and a Cache Redirection is put in place.
     * At this stage, the Source still exists as a physical object in Storage, and we need to keep its Cache around, pointing
     * to the old object.
     *
     * @param offset                   The offset within the StreamSegment to merge at.
     * @param sourceStreamSegmentIndex The Read Index to begin merging.
     * @throws ObjectClosedException    If the StreamSegmentReadIndex is closed.
     * @throws NullPointerException     If data is null.
     * @throws IllegalStateException    If the current StreamSegment is a child StreamSegment.
     * @throws IllegalArgumentException If the operation would cause writing beyond the StreamSegment's DurableLogLength.
     * @throws IllegalArgumentException If the offset is invalid (does not match the previous add offset).
     * @throws IllegalArgumentException If sourceStreamSegmentIndex refers to a StreamSegment that is already merged..
     * @throws IllegalArgumentException If sourceStreamSegmentIndex refers to a StreamSegment that has a different parent
     *                                  StreamSegment than the current index's one.
     */
    public void beginMerge(long offset, StreamSegmentReadIndex sourceStreamSegmentIndex) {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        if (this.metadata.getParentId() != SegmentMetadataCollection.NoStreamSegmentId) {
            throw new IllegalStateException("Cannot merge a StreamSegment into a child StreamSegment.");
        }

        if (sourceStreamSegmentIndex.isMerged()) {
            throw new IllegalArgumentException("Given ReadIndex is already merged.");
        }

        SegmentMetadata sourceMetadata = sourceStreamSegmentIndex.metadata;
        if (sourceMetadata.getParentId() != this.metadata.getId()) {
            throw new IllegalArgumentException("Given ReadIndex refers to a StreamSegment that does not have this ReadIndex's StreamSegment as a parent.");
        }

        if (!sourceMetadata.isSealed()) {
            throw new IllegalArgumentException("Given ReadIndex refers to a StreamSegment that is not sealed.");
        }

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
        if (endOffset > durableLogLength) {
            throw new IllegalArgumentException(String.format("The given range of bytes(%d-%d) is beyond the StreamSegment Durable Log Length (%d).", offset, endOffset, durableLogLength));
        }

        // Check and record the merger (optimistically).
        RedirectReadIndexEntry newEntry = new RedirectReadIndexEntry(offset, sourceLength, sourceStreamSegmentIndex);
        try (AutoReleaseLock ignored = lock.acquireWriteLock()) {
            if (this.mergeOffsets.containsKey(sourceMetadata.getId())) {
                throw new IllegalArgumentException("Given ReadIndex is already merged or in the process of being merged into this one.");
            }

            this.mergeOffsets.put(sourceMetadata.getId(), newEntry.getLastStreamSegmentOffset());
        }

        try {
            append(newEntry);
        }
        catch (Exception ex) {
            // If the merger failed, roll back the markers.
            try (AutoReleaseLock ignored = lock.acquireWriteLock()) {
                this.mergeOffsets.remove(sourceMetadata.getId());
            }

            throw ex;
        }
    }

    /**
     * Executes Step 2 of the 2-Step Merge Process.
     * The StreamSegments are physically merged in the Storage. The Source StreamSegment does not exist anymore.
     * The Cache entries of the two Streams are actually joined together.
     *
     * @param sourceSegmentStreamId
     */
    public void completeMerge(long sourceSegmentStreamId) {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        // Find the appropriate redirect entry.
        RedirectReadIndexEntry redirectEntry;
        long endOffset;
        try (AutoReleaseLock ignored = lock.acquireReadLock()) {
            endOffset = this.mergeOffsets.getOrDefault(sourceSegmentStreamId, -1L);
            if (endOffset < 0) {
                throw new IllegalArgumentException("Given ReadIndex's merger with this one has not been initiated using beginMerge. Cannot finalize the merger.");
            }

            ReadIndexEntry treeEntry = this.entries.getOrDefault(endOffset, null);
            if (treeEntry == null || !(treeEntry instanceof RedirectReadIndexEntry)) {
                throw new AssertionError(String.format("mergeOffsets points to a ReadIndexEntry that does not exist or is of the wrong type. sourceStreamSegmentId = %d, offset = %d, treeEntry = %s.", sourceSegmentStreamId, endOffset, treeEntry));
            }

            redirectEntry = (RedirectReadIndexEntry) treeEntry;
        }

        StreamSegmentReadIndex sourceIndex = redirectEntry.getRedirectReadIndex();
        SegmentMetadata sourceMetadata = sourceIndex.metadata;
        if (!sourceMetadata.isDeleted()) {
            throw new IllegalArgumentException("Given ReadIndex refers to a StreamSegment that has not been deleted yet.");
        }

        // TODO: an alternative to this is just drop the RedirectReadIndexEntry; next time we want to read, we'll just read from storage. That may be faster actually than just appending all these entries (there could be tens of thousands...)
        // Get all the entries from the source index and add them here. TODO: should we coalesce them too (into bigger entries)?
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
    }

    private void append(ReadIndexEntry entry) {
        try (AutoReleaseLock ignored = this.lock.acquireWriteLock()) {
            if (this.lastAppendedOffset >= 0 && entry.getStreamSegmentOffset() != this.lastAppendedOffset + 1) {
                throw new IllegalArgumentException(String.format("The given range of bytes (%d-%d) does not start right after the last appended range (%d).", entry.getStreamSegmentOffset(), entry.getLastStreamSegmentOffset(), this.lastAppendedOffset));
            }

            // Finally, add the entry.
            // Key is Offset + Length -1 = Last Offset Of Entry. Value is entry itself. This makes searching easier.
            ReadIndexEntry oldEntry = this.entries.put(entry.getLastStreamSegmentOffset(), entry);
            if (oldEntry != null) {
                throw new AssertionError(String.format("Added a new entry in the ReadIndex that overrode an existing element. New = %s, Old = %s.", entry, oldEntry));
            }

            this.lastAppendedOffset = entry.getLastStreamSegmentOffset();
        }
    }

    //endregion

    //region Reading

    /**
     * Triggers all future reads that have a starting offset before the given value.
     *
     * @throws ObjectClosedException If the object has been closed.
     * @throws IllegalStateException If the read index is in recovery mode.
     */
    public void triggerFutureReads() {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        if (this.recoveryMode) {
            throw new IllegalStateException("StreamSegmentReadIndex is in Recovery Mode.");
        }

        if (this.entries.size() == 0) {
            // Nothing to do.
            return;
        }

        // Get all eligible Future Reads which wait for data prior to the end offset.
        ReadIndexEntry lastEntry = this.entries.lastEntry().getValue(); //TODO: this is O(log(n)), not O(1)
        Collection<PlaceholderReadResultEntry> futureReads = this.futureReads.pollEntriesWithOffsetLessThan(lastEntry.getLastStreamSegmentOffset());

        for (PlaceholderReadResultEntry r : futureReads) {
            ReadResultEntry entry = getFirstReadResultEntry(r.getStreamSegmentOffset(), r.getRequestedReadLength());

            if (entry == null) {
                throw new AssertionError("Serving a PlaceholderReadResultEntry with a null result");
            }

            if (entry instanceof PlaceholderReadResultEntry) {
                throw new AssertionError("Serving a PlaceholderReadResultEntry with another PlaceholderReadResultEntry.");
            }

            if (entry.isEndOfStreamSegment()) {
                // We have attempted to read beyond the end of the stream. Fail the read request with the appropriate message.
                r.fail(new StreamSegmentSealedException(String.format("StreamSegment has been sealed at offset %d. There can be no more reads beyond this offset.", this.metadata.getDurableLogLength())));
            }
            else {
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
     * @throws ObjectClosedException    If the StreamSegmentReadIndex is closed.
     * @throws IllegalStateException    If the read index is in recovery mode.
     * @throws IllegalArgumentException If the parameters are invalid.
     * @throws IllegalArgumentException If the StreamSegment is sealed and startOffset is beyond its length.
     */
    public ReadResult read(long startOffset, int maxLength, Duration timeout) {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        if (this.recoveryMode) {
            throw new IllegalStateException("StreamSegmentReadIndex is in Recovery Mode.");
        }

        if (startOffset < 0) {
            throw new IllegalArgumentException("startOffset must be a non-negative number.");
        }

        if (maxLength < 0) {
            throw new IllegalArgumentException("maxLength must be a non-negative number.");
        }

        if (!canReadAtOffset(startOffset)) {
            throw new IllegalArgumentException("StreamSegment is sealed and startOffset is beyond the last offset of the StreamSegment.");
        }

        return new StreamSegmentReadResult(startOffset, maxLength, this::getFirstReadResultEntry);
    }

    private boolean canReadAtOffset(long offset) {
        if (this.metadata.isSealed() && offset > this.metadata.getDurableLogLength()) {
            return false;
        }

        return true;
    }

    /**
     * Returns the first ReadResultEntry that matches the specified search parameters.
     *
     * @param resultStartOffset The Offset within the StreamSegment where to start returning data from.
     * @param maxLength         The maximum number of bytes to return.
     * @return A ReadResultEntry representing the data to return.
     */
    private ReadResultEntry getFirstReadResultEntry(long resultStartOffset, int maxLength) {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }

        if (maxLength < 0) {
            // Nothing to read.
            return null;
        }

        // Check to see if we are trying to read beyond the last offset of a sealed StreamSegment.
        if (!canReadAtOffset(resultStartOffset)) {
            return new EndOfStreamSegmentReadResultEntry(resultStartOffset, maxLength);
        }

        try (AutoReleaseLock ignored = this.lock.acquireReadLock()) {
            if (this.entries.size() == 0) {
                // We have no entries in the Read Index.
                // Use the metadata to figure out whether to return a Storage or Future Read.
                return createPlaceholderRead(resultStartOffset, maxLength);
            }
            else {
                // We have at least one entry.
                // Find the first entry that has an End offset beyond equal to at least ResultStartOffset.
                Map.Entry<Long, ReadIndexEntry> treeEntry = this.entries.ceilingEntry(resultStartOffset);
                if (treeEntry == null) {
                    // The ResultStartOffset is beyond the End Offset of the last entry in the index.
                    // Use the metadata to figure out whether to return a Storage or Future Read, since we do not have
                    // this data in memory.
                    return createPlaceholderRead(resultStartOffset, maxLength);
                }
                else {
                    // We have an entry. Let's see if it's valid or not.
                    ReadIndexEntry currentEntry = treeEntry.getValue();
                    if (resultStartOffset < currentEntry.getStreamSegmentOffset()) {
                        // ResultStartOffset is before the Start Offset of this entry. This means either:
                        // 1. This is the first entry and ResultStartOffset is before it. OR
                        // 2. We have a gap in our entries, and ResultStartOffset is somewhere in there.
                        // We must issue a Storage Read to bring the data to us (with a readLength of up to the size of the gap).
                        int readLength = (int) Math.min(maxLength, currentEntry.getStreamSegmentOffset() - resultStartOffset);
                        return createStorageRead(resultStartOffset, readLength);
                    }
                    else if (currentEntry instanceof ByteArrayReadIndexEntry) {
                        // ResultStartOffset is after the StartOffset and before the End Offset of this entry.
                        // TODO should we coalesce multiple congruent entries together?
                        return createMemoryRead((ByteArrayReadIndexEntry) currentEntry, resultStartOffset, maxLength);
                    }
                    else if (currentEntry instanceof RedirectReadIndexEntry) {
                        return getRedirectedReadResultEntry(resultStartOffset, maxLength, (RedirectReadIndexEntry) currentEntry);
                    }
                }
            }
        }

        // We should never get in here if we coded this correctly.
        throw new AssertionError(String.format("Reached the end of getFirstReadResultEntry(id=%d, offset=%d, length=%d) with no plausible result in sight. This means we missed a case.", this.metadata.getId(), resultStartOffset, maxLength));
    }

    private ReadResultEntry getRedirectedReadResultEntry(long streamSegmentOffset, int maxLength, RedirectReadIndexEntry entry) {
        StreamSegmentReadIndex redirectedIndex = entry.getRedirectReadIndex();
        long redirectOffset = streamSegmentOffset - entry.getStreamSegmentOffset();
        if (redirectOffset < 0 || redirectOffset >= entry.getLength()) {
            throw new AssertionError(String.format(
                    "Redirected offset would be outside of the range of the Redirected StreamSegment. StreamSegmentOffset = %d, MaxLength = %d, Entry.StartOffset = %d, Entry.Length = %d, RedirectOffset = %d.",
                    streamSegmentOffset,
                    maxLength,
                    entry.getStreamSegmentOffset(),
                    entry.getLength(),
                    redirectOffset));
        }

        if (entry.getLength() < maxLength) {
            maxLength = (int) entry.getLength();
        }

        ReadResultEntry result = redirectedIndex.getFirstReadResultEntry(redirectOffset, maxLength);
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
    private PlaceholderReadResultEntry createPlaceholderRead(long streamSegmentOffset, int maxLength) {
        long storageLength = this.metadata.getStorageLength();
        if (streamSegmentOffset < storageLength) {
            // Requested data exists in Storage.
            // Determine actual read length (until Storage Length) and make sure it does not exceed maxLength.
            long actualReadLength = storageLength - streamSegmentOffset;
            if (actualReadLength > maxLength) {
                actualReadLength = maxLength;
            }

            return createStorageRead(streamSegmentOffset, (int) actualReadLength);
        }
        else {
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
    private ReadResultEntry createMemoryRead(ByteArrayReadIndexEntry entry, long streamSegmentOffset, int maxLength) {
        if (streamSegmentOffset < entry.getStreamSegmentOffset()) {
            throw new AssertionError(String.format("streamSegmentOffset{%d} < entry.getStreamSegmentOffset{%d}", streamSegmentOffset, entry.getStreamSegmentOffset()));
        }

        int entryOffset = (int) (streamSegmentOffset - entry.getStreamSegmentOffset());
        int length = Math.min(maxLength, entry.getData().length - entryOffset);
        if (length <= 0) {
            throw new AssertionError(String.format("length{%d} <= 0. streamSegmentOffset = %d, maxLength = %d, entry.offset = %d, entry.length = %d", length, streamSegmentOffset, maxLength, entry.getStreamSegmentOffset(), entry.getData().length));
        }

        return new MemoryReadResultEntry(entry, entryOffset, length);
    }

    /**
     * Creates a ReadResultEntry that is a placeholder for data that is not in memory, but exists in Storage.
     *
     * @param streamSegmentOffset
     * @param readLength
     * @return
     */
    private PlaceholderReadResultEntry createStorageRead(long streamSegmentOffset, int readLength) {
        // TODO: implement Storage Reads.
        return new PlaceholderReadResultEntry(streamSegmentOffset, readLength);
    }

    /**
     * Creates a ReadResultEntry that is a placeholder for data that is not in memory, or in storage, which has
     * a starting offset beyond the length of the StreamSegment.
     *
     * @param streamSegmentOffset
     * @param maxLength
     * @return
     */
    private PlaceholderReadResultEntry createFutureRead(long streamSegmentOffset, int maxLength) {
        PlaceholderReadResultEntry entry = new PlaceholderReadResultEntry(streamSegmentOffset, maxLength);
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
        if (offsetAdjustment < 0) {
            throw new IllegalArgumentException("offsetAdjustment must be a non-negative number.");
        }

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

