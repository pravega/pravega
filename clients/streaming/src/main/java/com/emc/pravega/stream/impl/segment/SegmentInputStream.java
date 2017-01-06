/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl.segment;

import com.emc.pravega.stream.EventStreamWriter;

import java.nio.ByteBuffer;

/**
 * Defines a InputStream for a single segment.
 * Once created the offset must be provided by calling setOffset.
 * The next read will proceed from this offset. Subsequent reads will read from where the previous
 * one left off. (Parallel calls to read data will be serialized)
 * Get offset can be used to store a location to revert back to that position in the future.
 */
public abstract class SegmentInputStream implements AutoCloseable {
    /**
     * Sets the offset for reading from the segment.
     *
     * @param offset The offset to set.
     */
    public abstract void setOffset(long offset);

    /**
     * Gets the current offset. (Passing this to setOffset in the future will reset reads to the
     * current position in the segment.)
     */
    public abstract long getOffset();
    
    /**
     * Returns the length of the current segment. i.e. calling setOffset with the result of this
     * method followed by read would result in read blocking until more data is written.
     */
    public abstract long fetchCurrentStreamLength();

    /**
     * Reads bytes from the segment a single event.
     * Buffering is performed internally to try to prevent blocking.
     *
     * @return A ByteBuffer containing the serialized data that was written via {@link EventStreamWriter#writeEvent(String, Object)}
     * @throws EndOfSegmentException If no event could be read because the end of the segment was reached.
     */
    public abstract ByteBuffer read() throws EndOfSegmentException;
    
    /**
     * Closes this InputStream. No further methods may be called after close.
     * This will free any resources associated with the InputStream.
     */
    @Override
    public abstract void close();
}