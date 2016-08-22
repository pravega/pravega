/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
	 * @param offset Sets the offset for reading from the segment.
	 */
	public abstract void setOffset(long offset);

    /**
     * @return The current offset. (Passing this to setOffset in the future will reset reads to the
     *         current position in the segment.)
     */
    public abstract long getOffset();
	
	/**
	 * @return The number of bytes that can be read by calling read without blocking.
	 */
	public abstract int available();
	
	/**
	 * Reads bytes from the segment to attempt to fill the provided buffer.
	 * Buffering is performed internally to try to prevent blocking.
	 * Similarly the bufferProvided may not be fully filled, if doing so can prevent further blocking. 
	 * The number of bytes that can be read without blocking can be obtained by calling available().
	 * 
	 * This method will always read at least one byte even if blocking is required to obtain it.
	 *  
	 * @param toFill A buffer to fill by reading from the segment
	 * @return the number of bytes read
	 * @throws EndOfSegmentException If no bytes were read because the end of the segment was reached.
	 */
	public abstract int read(ByteBuffer toFill) throws EndOfSegmentException;

	/**
	 * Close this InputStream. No further methods may be called after close. 
	 * This will free any resources associated with the InputStream.
	 */
	@Override
	public abstract void close();
}