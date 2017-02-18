/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;

import java.nio.ByteBuffer;

/**
 * The mirror of {@link EventStreamReader} but that is specific to a single segment.
 */
public interface SegmentEventReader extends AutoCloseable {
    Segment getSegmentId();

    /**
     * Return the next event, blocking for at most timeout.
     * If there is no event after timeout null will be returned.
     * EndOfSegmentException indicates the segment has ended an no more events may be read.
     *
     * @param timeout Timeout for the operation, in milliseconds.
     * @throws EndOfSegmentException If we reached the end of the segment.
     * @return The next event.
     */
    ByteBuffer getNextEvent(long timeout) throws EndOfSegmentException;

    /**
     * Returns the current offset. This can be passed to {@link #setOffset(long)} to restore to the current position.
     *
     * @return Current offset of the reader.
     */
    long getOffset();

    /**
     * Given an offset obtained from {@link SegmentEventReader#getOffset()} reset consumption to that position.
     *
     * @param offset The offset to set.
     */
    void setOffset(long offset);

    /**
     * Closes the reader. Frees any resources associated with it.
     *
     * No further opertations may be performed.
     */
    @Override
    void close();

    /**
     * Returns true if the data can be read from the local buffer without blocking the caller.
     *
     * @return false if data read is blocking.
     */
    boolean canReadWithoutBlocking();

}
