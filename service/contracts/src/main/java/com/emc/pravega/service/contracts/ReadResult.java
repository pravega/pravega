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

package com.emc.pravega.service.contracts;

import java.util.Iterator;

/**
 * Represents a Read Result from a Stream Segment. This is essentially an Iterator over smaller, continuous ReadResultEntries.
 * Calls to hasNext() will return true only if the following conditions are all met:
 * <ul>
 * <li> The ReadResult is not closed
 * <li> The ReadResult has not reached the end of a sealed StreamSegment
 * <li> The ReadResult has not consumed all requested data (getConsumedLength() < getMaxResultLength())
 * </ul>
 * <p>
 * Calls to next() will return an element as long as hasNext() returns true. Some notes to consider:
 * <ul>
 * <li> next() will very likely not return the whole requested read range at the same time, even if subsequent calls to it
 * return contiguous data.
 * <li> next() will return a specific instance of ReadResultEntry, depending on where the data to be read is located.
 * <li> If next() returns an entry that has "isEndOfStreamSegment()" returning true, it means the Read Result has reached
 * the end of a sealed stream, and subsequent calls to hasNext() will return false.
 * <li> If the data is readily available in memory, the returned ReadResultEntry will contain an already completed future,
 * ready for consumption (using the regular CompletableFuture methods).
 * <li> If the data is not available in memory (currently), the returned ReadResultEntry will contain a CompletableFuture
 * that will be completed when data becomes available (pulled from Storage or Future Read). If the process of retrieving
 * data fails, the CompletableFuture will fail with the source exception as cause.
 * <li> If the data requested is beyond the last offset of the StreamSegment (Future Read), the returned CompletableFuture
 * will complete when at least 1 byte of data from that offset is available, or will fail with a TimeoutException when the
 * timeout expires or StreamSegmentSealedException if the StreamSegment has been sealed at or before its offset.
 * </ul>
 */
public interface ReadResult extends Iterator<ReadResultEntry>, AutoCloseable {
    /**
     * Gets a value indicating the Offset within the StreamSegment where this ReadResult starts at.
     *
     * @return
     */
    long getStreamSegmentStartOffset();

    /**
     * Gets a value indicating the maximum length that this read result can have.
     *
     * @return
     */
    int getMaxResultLength();

    /**
     * Gets a value indicating the number of bytes that have been consumed via the next() method invocations.
     * Note that this does not track the individual consumption within the objects returned by next().
     *
     * @return
     */
    int getConsumedLength();

    /**
     * Gets a value indicating whether this ReadResult is fully consumed (either because it was read in its entirety
     * or because it was closed externally).
     *
     * @return
     */
    boolean isClosed();

    /**
     * Closes the ReadResult.
     */
    @Override
    void close();
}
