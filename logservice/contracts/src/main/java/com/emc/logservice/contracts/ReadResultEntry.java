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

package com.emc.logservice.contracts;

import java.util.concurrent.CompletableFuture;

/**
 * Base class for an Entry that makes up a ReadResult.
 */
public abstract class ReadResultEntry {
    //region Members

    private long streamSegmentOffset;
    private final int requestedReadLength;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ReadResultEntry class.
     *
     * @param streamSegmentOffset The offset in the StreamSegment that this entry starts at.
     * @param requestedReadLength The maximum number of bytes requested for read.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    protected ReadResultEntry(long streamSegmentOffset, int requestedReadLength) {
        if (streamSegmentOffset < 0) {
            throw new IllegalArgumentException("streamSegmentOffset must be a non-negative number.");
        }

        if (requestedReadLength <= 0) {
            throw new IllegalArgumentException("requestedReadLength must be a positive integer.");
        }

        this.streamSegmentOffset = streamSegmentOffset;
        this.requestedReadLength = requestedReadLength;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the offset in the StreamSegment that this entry starts at.
     *
     * @return
     */
    public long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    /**
     * Gets a value indicating the number of bytes requested for reading.
     * NOTE: The number of bytes actually read may differ from this value.
     *
     * @return
     */
    public int getRequestedReadLength() {
        return this.requestedReadLength;
    }

    /**
     * Gets a value indicating whether we have reached the end of the StreamSegment and we cannot read anymore.
     *
     * @return
     */
    public boolean isEndOfStreamSegment() {
        return false;
    }

    /**
     * Adjusts the offset by the given amount.
     * //TODO: this method should not be exposed here in the contracts package.
     * @param delta The amount to adjust by.
     * @throws IllegalArgumentException If the new offset would be negative.
     */
    public void adjustOffset(long delta) {
        long newOffset = this.streamSegmentOffset + delta;
        if (newOffset < 0) {
            throw new IllegalArgumentException("Given delta would result in a negative offset.");
        }

        this.streamSegmentOffset = newOffset;
    }

    /**
     * Gets the data.
     * TODO: this needs to be refactored so that when getContent() is invoked, the data is actually retrieved. This class
     * TODO: needs to take in another argument in the constructor that will be called when the content is successfully retrieved (with the contents itself, including length).
     * @return A CompletableFuture that, when completed, will contain the requested read result contents. If the operation
     * fails, the exception that triggered this will be stored here.
     */
    public abstract CompletableFuture<ReadResultEntryContents> getContent();

    @Override
    public String toString() {
        CompletableFuture<ReadResultEntryContents> contentFuture = getContent();
        return String.format("%s: Offset = %d, RequestedLength = %d, HasData = %s, Error = %s, Cancelled = %s",
                this.getClass().getSimpleName(),
                getStreamSegmentOffset(),
                getRequestedReadLength(),
                contentFuture.isDone() && !contentFuture.isCompletedExceptionally() && !contentFuture.isCancelled(),
                contentFuture.isCompletedExceptionally(),
                contentFuture.isCancelled());
    }

    //endregion
}
