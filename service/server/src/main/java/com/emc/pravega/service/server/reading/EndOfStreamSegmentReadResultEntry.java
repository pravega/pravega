/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.reading;

import com.emc.pravega.service.contracts.ReadResultEntryType;

import java.time.Duration;

/**
 * Read Result Entry with no content that marks the end of the StreamSegment.
 * The getContent() method will throw an IllegalStateException if invoked.
 */
class EndOfStreamSegmentReadResultEntry extends ReadResultEntryBase {
    /**
     * Constructor.
     *
     * @param streamSegmentOffset The offset in the StreamSegment that this entry starts at.
     * @param requestedReadLength The maximum number of bytes requested for read.
     */
    EndOfStreamSegmentReadResultEntry(long streamSegmentOffset, int requestedReadLength) {
        super(ReadResultEntryType.EndOfStreamSegment, streamSegmentOffset, requestedReadLength);
        fail(new IllegalStateException("EndOfStreamSegmentReadResultEntry does not have any content."));
    }

    @Override
    public void requestContent(Duration timeout) {
        throw new IllegalStateException("EndOfStreamSegmentReadResultEntry does not have any content.");
    }
}
