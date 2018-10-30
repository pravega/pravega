/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import io.pravega.client.stream.EventStreamWriter;

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
     * Reads bytes from the segment a single event. Buffering is performed internally to try to prevent
     * blocking. If there is no event after timeout null will be returned. EndOfSegmentException indicates the
     * segment has ended an no more events may be read.
     *
     * @return A ByteBuffer containing the serialized data that was written via
     *         {@link EventStreamWriter#writeEvent(String, Object)}
     * @throws EndOfSegmentException If no event could be read because the end of the segment was reached.
     * @throws SegmentTruncatedException If the segment has been truncated beyond the current offset and the data cannot be read.
     */
    public default ByteBuffer read() throws EndOfSegmentException, SegmentTruncatedException {
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
     * @throws SegmentTruncatedException If the segment has been truncated beyond the current offset and the data cannot be read.
     */
    public abstract ByteBuffer read(long firstByteTimeout) throws EndOfSegmentException, SegmentTruncatedException;
    
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
     * Returns true if {@link #read()} can be invoked without blocking. (This may be because there
     * is data in a buffer, or the call will throw EndOfSegmentException).
     *
     * @return False if data read is blocking.
     */
    public boolean isSegmentReady();
}
