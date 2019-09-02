/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.reading;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.ReadResultEntry;
import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import lombok.extern.slf4j.Slf4j;

/**
 * Represents a Read Result from a Stream Segment. This is essentially an Iterator over smaller, continuous ReadResultEntries.
 */
@Slf4j
class StreamSegmentReadResult implements ReadResult {
    //region Members

    private final String traceObjectId;
    private final long streamSegmentStartOffset;
    private final int maxResultLength;
    private final NextEntrySupplier getNextItem;
    private CompletableFuture<ReadResultEntryContents> lastEntryFuture;
    private int consumedLength;
    private boolean canRead;
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentReadResult class.
     *
     * @param streamSegmentStartOffset The StreamSegment Offset where the ReadResult starts at.
     * @param maxResultLength          The maximum number of bytes to read.
     * @param getNextItem              A Bi-Function that returns the next ReadResultEntry to consume. The first argument
     *                                 is startOffset (long) and the second is remainingLength (int).
     * @throws NullPointerException     If getNextItem is null.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    StreamSegmentReadResult(long streamSegmentStartOffset, int maxResultLength, NextEntrySupplier getNextItem, String traceObjectId) {
        Exceptions.checkArgument(streamSegmentStartOffset >= 0, "streamSegmentStartOffset", "streamSegmentStartOffset must be a non-negative number.");
        Exceptions.checkArgument(maxResultLength >= 0, "maxResultLength", "maxResultLength must be a non-negative number.");
        Preconditions.checkNotNull(getNextItem, "getNextItem");

        this.traceObjectId = traceObjectId;
        this.streamSegmentStartOffset = streamSegmentStartOffset;
        this.maxResultLength = maxResultLength;
        this.getNextItem = getNextItem;
        this.consumedLength = 0;
        this.canRead = true;
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
        return String.format("Offset = %d, MaxLength = %d, Consumed = %d", this.streamSegmentStartOffset, this.maxResultLength, this.consumedLength);
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
     */
    @Override
    public boolean hasNext() {
        return !this.closed && this.canRead && this.consumedLength < this.maxResultLength;
    }

    /**
     * Gets the next ReadResultEntry that exists in the ReadResult. This will return null if hasNext() indicates false
     * (as opposed from throwing a NoSuchElementException, like a general iterator).
     *
     * Notes:
     * <ul>
     * <li> Calls to next() will block until the ReadResultEntry.getContent() for the previous call to next() has been completed (normally or exceptionally).
     * <li> Due to a result from next() only being considered "consumed" after its getContent() is completed, it is possible
     * that hasNext() will report true, but a subsequent call to next() will return null - that's because next() will wait
     * for the previous entry to complete and then do more processing.
     * </ul>
     *
     * @throws IllegalStateException If we have more elements, but the last element returned hasn't finished processing.
     */
    @Override
    public ReadResultEntry next() {
        Exceptions.checkNotClosed(this.closed, this);

        // If the previous entry hasn't finished yet, we cannot proceed.
        Preconditions.checkState(this.lastEntryFuture == null || this.lastEntryFuture.isDone(), "Cannot request a new entry when the previous one hasn't completed retrieval yet.");
        if (this.lastEntryFuture != null && !this.lastEntryFuture.isDone()) {
            this.lastEntryFuture.join();
        }

        // Only check for hasNext now, after we have waited for the previous entry to finish - since that updates
        // some fields that hasNext relies on.
        if (!hasNext()) {
            return null;
        }

        // Retrieve the next item.
        long startOffset = this.streamSegmentStartOffset + this.consumedLength;
        int remainingLength = this.maxResultLength - this.consumedLength;
        CompletableReadResultEntry entry = this.getNextItem.apply(startOffset, remainingLength);

        if (entry == null) {
            assert remainingLength <= 0 : String.format("No ReadResultEntry received when one was expected. Offset %d, MaxLen %d.", startOffset, remainingLength);
            this.lastEntryFuture = null;
        } else {
            assert entry.getStreamSegmentOffset() == startOffset : String.format("Invalid ReadResultEntry. Expected offset %d, given %d.", startOffset, entry.getStreamSegmentOffset());
            if (entry.getType().isTerminal()) {
                // We encountered Terminal Entry, which means we cannot read anymore using this ReadResult.
                // This can be the case if either the StreamSegment has been truncated beyond the current ReadResult offset,
                // or if the StreamSegment is now sealed and we have requested an offset that is beyond the StreamSegment
                // length. We cannot continue reading; close the ReadResult and return the appropriate Result Entry.
                // If we don't close the ReadResult, hasNext() will erroneously return true and next() will have undefined behavior.
                this.lastEntryFuture = null;
                this.canRead = false;
            } else {
                // After the previous entry is done, update the consumedLength value.
                entry.setCompletionCallback(length -> this.consumedLength += length);
                this.lastEntryFuture = entry.getContent();

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

    /**
     * Defines a Function that given a startOffset (long) and remainingLength (int), returns the next entry to be consumed (ReadResultEntry).
     */
    @FunctionalInterface
    interface NextEntrySupplier extends BiFunction<Long, Integer, CompletableReadResultEntry> {
    }

    //endregion
}
