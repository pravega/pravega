/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl.segment;

import io.pravega.stream.EventStreamWriter;
import io.pravega.stream.Segment;

import java.nio.ByteBuffer;

/**
 * Defines a InputStream for a single segment. 
 * Once created the offset must be provided by calling setOffset.
 * The next read will proceed from this offset. Subsequent reads will read from where the previous
 * one left off. (Parallel calls to read data will be serialized)
 * Get offset can be used to store a location to revert back to that position in the future.
 */
public interface SegmentInputStream extends AutoCloseable {
    
    Segment getSegmentId();
    
    /**
     * Sets the offset for reading from the segment.
     *
     * @param offset The offset to set.
     */
    public abstract void setOffset(long offset);

    /**
     * Gets the current offset. (Passing this to setOffset in the future will reset reads to the
     * current position in the segment.)
     *
     * @return The current offset.
     */
    public abstract long getOffset();
    
    /**
     * Returns the length of the current segment. i.e. calling setOffset with the result of this
     * method followed by read would result in read blocking until more data is written.
     *
     * @return The length of the current segment.
     */
    public abstract long fetchCurrentStreamLength();
    
    /**
     * Reads bytes from the segment a single event. Buffering is performed internally to try to prevent
     * blocking. If there is no event after timeout null will be returned. EndOfSegmentException indicates the
     * segment has ended an no more events may be read.
     *
     * @return A ByteBuffer containing the serialized data that was written via
     *         {@link EventStreamWriter#writeEvent(String, Object)}
     * @throws EndOfSegmentException If no event could be read because the end of the segment was reached.
     */
    public default ByteBuffer read() throws EndOfSegmentException {
        return read(Long.MAX_VALUE);
    }
    
    /**
     * Reads bytes from the segment a single event. Buffering is performed internally to try to prevent
     * blocking. If there is no event after timeout null will be returned. EndOfSegmentException indicates the
     * segment has ended an no more events may be read.
     * 
     * A timeout can be provided that will be used to determine how long to block obtaining the first byte of
     * an event. If this timeout elapses null is returned. Once an event has been partially read it will be
     * fully read without regard to the timeout.
     *
     * @param firstByteTimeout The maximum length of time to block to get the first byte of the event.
     * @return A ByteBuffer containing the serialized data that was written via
     *         {@link EventStreamWriter#writeEvent(String, Object)}
     * @throws EndOfSegmentException If no event could be read because the end of the segment was reached.
     */
    public abstract ByteBuffer read(long firstByteTimeout) throws EndOfSegmentException;
    
    /**
     * Issue a request to asynchronously fill the buffer. To hopefully prevent future {@link #read()} calls from blocking.
     * Calling this multiple times is harmless.
     */
    public abstract void fillBuffer();
    
    /**
     * Closes this InputStream. No further methods may be called after close.
     * This will free any resources associated with the InputStream.
     */
    @Override
    public abstract void close();
    
    /**
     * Returns true if the data can be read from the local buffer without blocking the caller.
     *
     * @return False if data read is blocking.
     */
    public boolean canReadWithoutBlocking();
}
