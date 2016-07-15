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

package com.emc.logservice.server.reading;

import com.emc.logservice.common.Exceptions;
import com.emc.logservice.contracts.ReadResult;
import com.emc.logservice.contracts.ReadResultEntry;
import com.emc.logservice.contracts.ReadResultEntryContents;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Represents a Read Result from a Stream Segment. This is essentially an Iterator over smaller, continuous ReadResultEntries.
 */
@Slf4j
public class StreamSegmentReadResult implements ReadResult {
    //region Members

    private final String traceObjectId;
    private final long streamSegmentStartOffset;
    private final int maxResultLength;
    private final NextEntrySupplier getNextItem;
    private CompletableFuture<ReadResultEntryContents> lastEntryFuture;
    private CompletableFuture<Void> lastEntryFutureFollowup;
    private int consumedLength;
    private boolean endOfSegmentReached;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentReadResult class.
     *
     * @param streamSegmentStartOffset The StreamSegment Offset where the ReadResult starts at.
     * @param maxResultLength          The maximum number of bytes to read.
     * @param getNextItem              A Bi-Function that returns the next ReadResultEntry to consume.
     * @throws NullPointerException     If getNextItem is null.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    protected StreamSegmentReadResult(long streamSegmentStartOffset, int maxResultLength, NextEntrySupplier getNextItem, String traceObjectId) {
        Exceptions.checkArgument(streamSegmentStartOffset >= 0, "streamSegmentStartOffset", "streamSegmentStartOffset must be a non-negative number.");
        Exceptions.checkArgument(maxResultLength >= 0, "maxResultLength", "maxResultLength must be a non-negative number.");
        Preconditions.checkNotNull(getNextItem, "getNextItem");

        this.traceObjectId = traceObjectId;
        this.streamSegmentStartOffset = streamSegmentStartOffset;
        this.maxResultLength = maxResultLength;
        this.getNextItem = getNextItem;
        this.consumedLength = 0;
    }

    //endregion

    //region ReadResult Implementation

    @Override
    public long getStreamSegmentStartOffset() {
        return this.streamSegmentStartOffset;
    }

    @Override
    public int getMaxResultLength() {
        return this.maxResultLength;
    }

    @Override
    public int getConsumedLength() {
        return this.consumedLength;
    }

    @Override
    public boolean isClosed() {
        return this.closed || !hasNext();
    }

    @Override
    public String toString() {
        return String.format("Offset = %d, MaxLength = %d, Consumed = %d", getStreamSegmentStartOffset(), getMaxResultLength(), getConsumedLength());
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;

            // If we have already returned a result but it hasn't been consumed yet, cancel it.
            CompletableFuture<ReadResultEntryContents> lastReturnedFuture = this.lastEntryFuture;
            if (lastReturnedFuture != null && !lastReturnedFuture.isDone()) {
                lastReturnedFuture.cancel(true);
                this.lastEntryFuture = null;
            }

            log.trace("{}.ReadResult[{}]: Closed.", this.traceObjectId, this.streamSegmentStartOffset);
        }
    }

    //endregion

    //region Iterator Implementation

    /**
     * Gets a value indicating whether we have further elements to process. All of the following must be true:
     * <ul>
     * <li> The StreamSegmentReadResult is not closed.
     * <li> We haven't reached the end of a sealed StreamSegment.
     * <li> We have at least one more byte to read.
     * </ul>
     *
     * @return
     */
    @Override
    public boolean hasNext() {
        return !this.closed && !this.endOfSegmentReached && this.consumedLength < this.maxResultLength;
    }

    /**
     * Gets the next ReadResultEntry that exists in the ReadResult. This will return an element only if hasNext() returns true.
     *
     * @return
     * @throws IllegalStateException            If we have more elements, but the last element returned hasn't finished processing.
     * @throws java.util.NoSuchElementException If hasNext() returns false.
     */
    @Override
    public ReadResultEntry next() {
        Exceptions.checkNotClosed(this.closed, this);

        if (!hasNext()) {
            throw new NoSuchElementException("StreamSegmentReadResult has been read in its entirety.");
        }

        // If the previous entry hasn't finished yet, we cannot proceed.
        Preconditions.checkState(this.lastEntryFuture == null || this.lastEntryFuture.isDone(), "Cannot request a new entry when the previous one hasn't completed retrieval yet.");
        if (this.lastEntryFutureFollowup != null && !this.lastEntryFutureFollowup.isDone()) {
            // This is the follow-up code that we have from the previous execution. Even though the previous future may
            // have finished executing, the follow-up may not have, so wait for that as well.
            this.lastEntryFutureFollowup.join();
        }

        // Retrieve the next item.
        long startOffset = this.streamSegmentStartOffset + this.consumedLength;
        int remainingLength = this.maxResultLength - this.consumedLength;
        ReadResultEntry entry = this.getNextItem.apply(startOffset, remainingLength);

        if (entry == null) {
            assert remainingLength <= 0 : String.format("No ReadResultEntry received when one was expected. Offset %d, MaxLen %d.", startOffset, remainingLength);
            this.lastEntryFuture = null;
        } else {
            assert entry.getStreamSegmentOffset() == startOffset : String.format("Invalid ReadResultEntry. Expected offset %d, given %d.", startOffset, entry.getStreamSegmentOffset());
            if (entry.isEndOfStreamSegment()) {
                // StreamSegment is now sealed and we have requested an offset that is beyond the StreamSegment length.
                // We cannot continue reading; close the ReadResult and return the appropriate EndOfStream Result Entry.
                // If we don't close the ReadResult, hasNext() will erroneously return true and next() will have undefined behavior.
                this.lastEntryFuture = null;
                this.lastEntryFutureFollowup = null;
                this.endOfSegmentReached = true;
            } else {
                // After the previous entry is done, update the consumedLength value.
                this.lastEntryFuture = entry.getContent();
                this.lastEntryFutureFollowup = this.lastEntryFuture.thenAccept(contents -> this.consumedLength += contents.getLength());

                // Check, again, if we are closed. It is possible that this Result was closed after the last check
                // and before we got the lastEntryFuture. If this happened, throw the exception and don't return anything.
                Exceptions.checkNotClosed(this.closed, this);
            }
        }

        log.trace("{}.ReadResult[{}]: Consumed = {}, MaxLength = {}, Entry = ({}).", this.traceObjectId, this.streamSegmentStartOffset, this.consumedLength, this.maxResultLength, entry);
        return entry;
    }

    //endregion

    //region NextEntrySupplier

    @FunctionalInterface
    interface NextEntrySupplier extends BiFunction<Long, Integer, ReadResultEntry> {
    }

    //endregion
}
