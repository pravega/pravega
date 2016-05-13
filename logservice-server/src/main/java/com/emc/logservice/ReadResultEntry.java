package com.emc.logservice;

import java.util.concurrent.CompletableFuture;

/**
 * Base class for an Entry that makes up a ReadResult.
 */
public abstract class ReadResultEntry
{
    //region Members

    private long streamSegmentOffset;
    private final int requestedReadLength;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ReadResultEntry class.
     *
     * @param streamSegmentOffset The offset in the StreamSegment that this entry starts at.
     * @param requestedReadLength The maximum number of bytes requested for read.
     */
    protected ReadResultEntry(long streamSegmentOffset, int requestedReadLength)
    {
        this.streamSegmentOffset = streamSegmentOffset;
        this.requestedReadLength = requestedReadLength;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the offset in the StreamSegment that this entry starts at.
     *
     * @return
     */
    public long getStreamSegmentOffset()
    {
        return this.streamSegmentOffset;
    }

    /**
     * Gets a value indicating the number of bytes requested for reading.
     * NOTE: The number of bytes actually read may differ from this value.
     *
     * @return
     */
    public int getRequestedReadLength()
    {
        return this.requestedReadLength;
    }

    /**
     * Gets a value indicating whether we have reached the end of the StreamSegment and we cannot read anymore.
     *
     * @return
     */
    public boolean isEndOfStreamSegment()
    {
        return false;
    }

    /**
     * Adjusts the offset by the given amount.
     *
     * @param delta The amount to adjust by.
     * @throws IllegalArgumentException If the new offset would be negative.
     */
    public void adjustOffset(long delta)
    {
        long newOffset = this.streamSegmentOffset + delta;
        if (newOffset < 0)
        {
            throw new IllegalArgumentException("Given delta would result in a negative offset.");
        }

        this.streamSegmentOffset = newOffset;
    }

    /**
     * Gets the data.
     *
     * @return A CompletableFuture that, when completed, will contain the requested read result contents. If the operation
     * fails, the exception that triggered this will be stored here.
     */
    public abstract CompletableFuture<ReadResultEntryContents> getContent();

    @Override
    public String toString()
    {
        CompletableFuture<ReadResultEntryContents> contentFuture = getContent();
        return String.format("Offset = %d, RequestedLength = %d, HasData = %s, Error = %s, Cancelled = %s,",
                getStreamSegmentOffset(),
                getRequestedReadLength(),
                contentFuture.isDone() && !contentFuture.isCompletedExceptionally() && !contentFuture.isCancelled(),
                contentFuture.isCompletedExceptionally(),
                contentFuture.isCancelled());
    }

    //endregion
}
