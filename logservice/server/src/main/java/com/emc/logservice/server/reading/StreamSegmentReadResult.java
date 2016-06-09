package com.emc.logservice.server.reading;

import com.emc.logservice.common.Exceptions;
import com.emc.logservice.contracts.*;
import com.emc.logservice.common.ObjectClosedException;
import lombok.extern.slf4j.Slf4j;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Represents a Read Result from a Stream Segment. This is essentially an Iterator over smaller, continuous ReadResultEntries.
 */
@Slf4j
public class StreamSegmentReadResult implements ReadResult {
    //region Members

    private final String traceObjectId;
    private final long streamSegmentStartOffset;
    private final int maxResultLength;
    private final NextEntrySupplier getNextItem;
    private CompletableFuture<ReadResultEntryContents> lastEntryFuture;
    private CompletableFuture<Void> lastEntryFutureFollowup;
    private int consumedLength;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentReadResult class.
     *
     * @param streamSegmentStartOffset The StreamSegment Offset where the ReadResult starts at.
     * @param maxResultLength          The maximum number of bytes to read.
     * @param getNextItem              A Bi-Function that returns the next ReadResultEntry to consume.
     * @throws NullPointerException     If getNextItem is null.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    protected StreamSegmentReadResult(long streamSegmentStartOffset, int maxResultLength, NextEntrySupplier getNextItem, String traceObjectId) {
        Exceptions.throwIfIllegalArgument(streamSegmentStartOffset >= 0, "streamSegmentStartOffset", "streamSegmentStartOffset must be a non-negative number.");
        Exceptions.throwIfIllegalArgument(maxResultLength >= 0, "maxResultLength", "maxResultLength must be a non-negative number.");
        Exceptions.throwIfNull(getNextItem, "getNextItem");

        this.traceObjectId = traceObjectId;
        this.streamSegmentStartOffset = streamSegmentStartOffset;
        this.maxResultLength = maxResultLength;
        this.getNextItem = getNextItem;
        this.consumedLength = 0;
    }

    //endregion

    //region ReadResult Implementation

    @Override
    public long getStreamSegmentStartOffset() {
        return this.streamSegmentStartOffset;
    }

    @Override
    public int getMaxResultLength() {
        return this.maxResultLength;
    }

    @Override
    public int getConsumedLength() {
        return this.consumedLength;
    }

    @Override
    public boolean isClosed() {
        return this.closed || !hasNext();
    }

    @Override
    public String toString() {
        return String.format("Offset = %d, MaxLength = %d, Consumed = %d", getStreamSegmentStartOffset(), getMaxResultLength(), getConsumedLength());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        //TODO: should we also cancel any pending ReadResults?
        this.closed = true;
        log.trace("{}.ReadResult[{}]: Closed.", this.traceObjectId, this.streamSegmentStartOffset);
    }

    //endregion

    //region Iterator Implementation

    /**
     * Gets a value indicating whether we have further elements to process - which happens when we have at least one more byte to read.
     * If the ReadResult is closed, then this method will return false.
     *
     * @return
     */
    @Override
    public boolean hasNext() {
        return this.closed || this.consumedLength < this.maxResultLength;
    }

    /**
     * Gets the next ReadResultEntry that exists in the ReadResult. This will return an element only if hasNext() returns true.
     *
     * @return
     * @throws ObjectClosedException            If the StreamSegmentReadResult is closed.
     * @throws IllegalStateException            If we have more elements, but the last element returned hasn't finished processing.
     * @throws java.util.NoSuchElementException If hasNext() returns false.
     */
    @Override
    public ReadResultEntry next() {
        Exceptions.throwIfClosed(this.closed, this);

        if (!hasNext()) {
            throw new NoSuchElementException("StreamSegmentReadResult has been read in its entirety.");
        }

        // If the previous entry hasn't finished yet, we cannot proceed.
        Exceptions.throwIfIllegalState(this.lastEntryFuture == null || this.lastEntryFuture.isDone(), "Cannot request a new entry when the previous one hasn't completed retrieval yet.");
        if (this.lastEntryFutureFollowup != null && !this.lastEntryFutureFollowup.isDone()) {
            // This is the follow-up code that we have from the previous execution. Even though the previous future may
            // have finished executing, the follow-up may not have, so wait for that as well.
            this.lastEntryFutureFollowup.join();
        }

        // Retrieve the next item.
        long startOffset = this.streamSegmentStartOffset + this.consumedLength;
        int remainingLength = this.maxResultLength - this.consumedLength;
        ReadResultEntry entry = this.getNextItem.apply(startOffset, remainingLength);

        if (entry == null) {
            if (remainingLength > 0) {
                // We expected something but got nothing back.
                throw new AssertionError(String.format("No ReadResultEntry received when one was expected. Offset %d, MaxLen %d.", startOffset, remainingLength));
            }

            this.lastEntryFuture = null;
        }
        else if (entry.getStreamSegmentOffset() != startOffset) {
            // We got back a different offset than requested.
            throw new AssertionError(String.format("Invalid ReadResultEntry. Expected offset %d, given %d.", startOffset, entry.getStreamSegmentOffset()));
        }
        else if (entry.isEndOfStreamSegment()) {
            // StreamSegment is now sealed and we have requested an offset that is beyond the StreamSegment length.
            // We cannot continue reading; close the ReadResult and return the appropriate EndOfStream Result Entry.
            this.lastEntryFuture = null;
            this.lastEntryFutureFollowup = null;
            close();
        }
        else {
            // After the previous entry is done, update the consumedLength value.
            this.lastEntryFuture = entry.getContent();
            this.lastEntryFutureFollowup = this.lastEntryFuture.thenAccept(contents -> this.consumedLength += contents.getLength());
        }

        log.trace("{}.ReadResult[{}]: Consumed = {}, MaxLength = {}, Entry = ({}).", this.traceObjectId, this.streamSegmentStartOffset, this.consumedLength, this.maxResultLength, entry);
        return entry;
    }

    //endregion

    //region NextEntrySupplier

    public interface NextEntrySupplier extends BiFunction<Long, Integer, ReadResultEntry> {
    }

    //endregion
}
