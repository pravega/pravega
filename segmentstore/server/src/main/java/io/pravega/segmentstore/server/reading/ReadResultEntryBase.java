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

import io.pravega.segmentstore.contracts.ReadResultEntryContents;
import io.pravega.segmentstore.contracts.ReadResultEntryType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Base implementation of ReadResultEntry.
 */
@VisibleForTesting
public abstract class ReadResultEntryBase implements CompletableReadResultEntry {
    //region Members

    private final CompletableFuture<ReadResultEntryContents> contents;
    private final ReadResultEntryType type;
    private final int requestedReadLength;
    private final long streamSegmentOffset;
    private CompletionConsumer completionCallback;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ReadResultEntry class.
     *
     * @param type                The type of this ReadResultEntry.
     * @param streamSegmentOffset The offset in the StreamSegment that this entry starts at.
     * @param requestedReadLength The maximum number of bytes requested for read.
     * @throws IllegalArgumentException If any of the arguments are invalid.
     */
    protected ReadResultEntryBase(ReadResultEntryType type, long streamSegmentOffset, int requestedReadLength) {
        Preconditions.checkArgument(streamSegmentOffset >= 0, "streamSegmentOffset must be a non-negative number.");
        Preconditions.checkArgument(requestedReadLength > 0, "requestedReadLength must be a positive integer.");

        this.type = type;
        this.streamSegmentOffset = streamSegmentOffset;
        this.requestedReadLength = requestedReadLength;
        this.contents = new CompletableFuture<>();
    }

    //endregion

    //region ReadResultEntry Implementation

    @Override
    public long getStreamSegmentOffset() {
        return this.streamSegmentOffset;
    }

    @Override
    public int getRequestedReadLength() {
        return this.requestedReadLength;
    }

    @Override
    public ReadResultEntryType getType() {
        return this.type;
    }

    @Override
    public final CompletableFuture<ReadResultEntryContents> getContent() {
        return this.contents;
    }

    @Override
    public void requestContent(Duration timeout) {
        // This method intentionally left blank, to be implemented by derived classes that need it.
    }

    @Override
    public void setCompletionCallback(CompletionConsumer completionCallback) {
        this.completionCallback = completionCallback;
        if (completionCallback != null && this.contents.isDone() && !this.contents.isCompletedExceptionally()) {
            completionCallback.accept(this.contents.join().getLength());
        }
    }

    @Override
    public CompletionConsumer getCompletionCallback() {
        return this.completionCallback;
    }

    //endregion

    /**
     * Completes the Future of this ReadResultEntry by setting the given content.
     *
     * @param readResultEntryContents The content to set.
     */
    protected void complete(ReadResultEntryContents readResultEntryContents) {
        Preconditions.checkState(!this.contents.isDone(), "ReadResultEntry has already had its result set.");
        CompletionConsumer callback = this.completionCallback;
        if (callback != null) {
            callback.accept(readResultEntryContents.getLength());
        }

        this.contents.complete(readResultEntryContents);
    }

    /**
     * Fails the Future of this ReadResultEntry with the given exception.
     *
     * @param exception The exception to set.
     */
    protected void fail(Throwable exception) {
        Preconditions.checkState(!this.contents.isDone(), "ReadResultEntry has already had its result set.");
        this.contents.completeExceptionally(exception);
    }

    @Override
    public String toString() {
        CompletableFuture<ReadResultEntryContents> contentFuture = this.contents;
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
