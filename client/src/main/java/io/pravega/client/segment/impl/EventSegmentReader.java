/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.segment.impl;

import io.pravega.client.stream.EventStreamWriter;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Defines an event reader for a single segment.
 * Once created a offset may be provided by calling setOffset if the application does not want to read from offset 0.
 * The next read will proceed from this offset. Subsequent reads will read from where the previous
 * read call on this instance left off. (Parallel calls to read data will be serialized)
 * Get offset can be used to store a location to revert back to that position in the future.
 */
public interface EventSegmentReader extends AutoCloseable {
    
    Segment getSegmentId();
    
    /**
     * Sets the offset for reading from the segment.
     *
     * @param offset The offset to set.
     */
    default void setOffset(long offset) {
        setOffset(offset, false);
    }

    /**
     * Sets the offset for reading from the segment.
     *
     * @param offset The offset to set.
     * @param resendRequest Resend the read request incase there is an already pending read request for the offset.
     */
    void setOffset(long offset, boolean resendRequest);

    /**
     * Gets the current offset. (Passing this to setOffset in the future will reset reads to the
     * current position in the segment.)
     *
     * @return The current offset.
     */
    public abstract long getOffset();

    /**
     * Reads the bytes of a single event from the segment. Buffering is performed internally to try to prevent
     * blocking. If there is no event after timeout, then it returns null. EndOfSegmentException indicates the
     * segment has ended and there are no more events to read.
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
     * Reads bytes of a single event from the segment. Buffering is performed internally to try to prevent
     * blocking. If there is no event after timeout, then it returns null. EndOfSegmentException indicates the
     * segment has ended and there are no more events to read.
     * 
     * A timeout can be provided that will be used to determine how long to block obtaining the first byte of
     * an event. If this timeout elapses, then null is returned. Once an event has been partially read, it will be
     * fully read without regard to the timeout.
     *
     * @param firstByteTimeoutMillis The maximum length of time to block to get the first byte of the event.
     * @return A ByteBuffer containing the serialized data that was written via
     *         {@link EventStreamWriter#writeEvent(String, Object)}
     * @throws EndOfSegmentException If no event could be read because the end of the segment was reached.
     * @throws SegmentTruncatedException If the segment has been truncated beyond the current offset and the data cannot be read.
     */
    public abstract ByteBuffer read(long firstByteTimeoutMillis) throws EndOfSegmentException, SegmentTruncatedException;
    
    /**
     * Issues a request to asynchronously fill up the buffer. The goal is to prevent future {@link #read()} calls from blocking.
     * Calling this multiple times is harmless.
     * 
     * @return A future that completes when the request to fill the buffer has returned.
     */
    public abstract CompletableFuture<?> fillBuffer();
    
    /**
     * Closes this reader. No further methods may be called after close.
     * This will free any resources associated with the reader.
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
