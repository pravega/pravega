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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

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
    default void setOffset( long offset) {
        setOffset(offset, false);
    }

    /**
     * Sets the offset for reading from the segment.
     * @param offset The offset to set.
     * @param resendRequest Resend the read request in-case there is an already pending read request for the offset.
     */
    public abstract void setOffset(long offset, boolean resendRequest);

    /**
     * Gets the current offset. (Passing this to setOffset in the future will reset reads to the
     * current position in the segment.)
     *
     * @return The current offset.
     */
    public abstract long getOffset();

    /**
     * If data is available this will copy bytes from an internal buffer into the buffer provided.
     * If the provided buffer cannot be fully filled, it will return the data it has. If no data is
     * available it will block until some becomes available up to the provided timeout. A caller can
     * determine if this call will block in advance by calling {@link #bytesInBuffer()}. If a caller
     * wants to avoid blocking they call {@link #fillBuffer()} and use the future to be
     * notified when more data can be read without blocking.
     *
     * @param toFill the buffer to fill.
     * @param timeout the maximum time to block if no data is in memory.
     * @return The number of bytes read.
     * @throws EndOfSegmentException If no data could be read because the end of the segment was
     *             reached.
     * @throws SegmentTruncatedException If the segment has been truncated beyond the current offset
     *             and data cannot be read.
     */
    public abstract int read(ByteBuffer toFill, long timeout) throws EndOfSegmentException, SegmentTruncatedException;
    
    /**
     * Issue a request to asynchronously fill the buffer. To hopefully prevent future {@link #read(ByteBuffer, long)} calls from blocking.
     * Calling this multiple times is harmless.
     * 
     * @return A future that will be completed when there is data available to read.
     */
    public abstract CompletableFuture<?> fillBuffer();
    
    /**
     * Closes this InputStream. No further methods may be called after close.
     * This will free any resources associated with the InputStream.
     */
    @Override
    public abstract void close();
    
    /**
     * Returns &gt; 0 if {@link #read(ByteBuffer, long)} can be invoked without blocking.
     * Returns 0 if {@link #read(ByteBuffer, long)}  will block.
     * Returns -1 if a call to read will throw EndOfSegmentException.
     *
     * @return 0 if data read is blocking.
     */
    public int bytesInBuffer();
}
