package com.emc.logservice.reading;

import com.emc.logservice.ReadResultEntry;
import com.emc.logservice.ReadResultEntryContents;

import java.util.concurrent.CompletableFuture;

/**
 * Read Result Entry for data that is not readily available in memory. This data may be in Storage, or it may be for an
 * offset that is beyond the StreamSegment's DurableLogLength.
 */
public class PlaceholderReadResultEntry extends ReadResultEntry {
    private final CompletableFuture<ReadResultEntryContents> result;

    /**
     * Creates a new instance of the PlaceholderReadResultEntry class.
     *
     * @param streamSegmentOffset The offset in the StreamSegment that this entry starts at.
     * @param requestedReadLength The maximum number of bytes requested for read.
     */
    public PlaceholderReadResultEntry(long streamSegmentOffset, int requestedReadLength) {
        super(streamSegmentOffset, requestedReadLength);
        this.result = new CompletableFuture<>();
    }

    /**
     * Indicates that his placeholder read result entry can be completed with data that is now readily available.
     *
     * @param contents The contents of this read result.
     */
    protected void complete(ReadResultEntryContents contents) {
        this.result.complete(contents);
    }

    /**
     * Cancels this pending read result entry.
     */
    protected void cancel() {
        this.result.cancel(true);
    }

    /**
     * Indicates that this placeholder read result entry cannot be fulfilled and is cancelled with the given exception as cause.
     *
     * @param cause The reason why the read was cancelled.
     */
    protected void fail(Throwable cause) {
        this.result.completeExceptionally(cause);
    }

    //region ReadResultEntry Implementation

    @Override
    public CompletableFuture<ReadResultEntryContents> getContent() {
        return result;
    }

    //endregion
}
