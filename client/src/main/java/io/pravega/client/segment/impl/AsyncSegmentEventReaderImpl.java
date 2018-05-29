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

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.InvalidMessageException;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import lombok.Synchronized;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.GuardedBy;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static io.pravega.client.segment.impl.EndOfSegmentException.ErrorType.END_OFFSET_REACHED;

@Slf4j
@ToString
public class AsyncSegmentEventReaderImpl implements AsyncSegmentEventReader {
    static final int DEFAULT_READ_LENGTH = 64 * 1024;
    static final long UNBOUNDED_END_OFFSET = Long.MAX_VALUE;

    private final AsyncSegmentInputStream asyncInput;
    private final long endOffset;
    private final int readLength;

    @GuardedBy("$lock")
    private long cursorOffset;
    @GuardedBy("$lock")
    private final ReadState readState;
    @GuardedBy("$lock")
    private CompletableFuture<SegmentRead> outstandingRequest = null;

    AsyncSegmentEventReaderImpl(AsyncSegmentInputStream asyncInput, long startOffset) {
        this(asyncInput, startOffset, UNBOUNDED_END_OFFSET, DEFAULT_READ_LENGTH);
    }

    AsyncSegmentEventReaderImpl(AsyncSegmentInputStream asyncInput, long startOffset, long endOffset, int readLength) {
        Preconditions.checkArgument(startOffset >= 0);
        Preconditions.checkNotNull(asyncInput);
        Preconditions.checkNotNull(endOffset, "endOffset");
        Preconditions.checkArgument(endOffset > startOffset + WireCommands.TYPE_PLUS_LENGTH_SIZE,
                "Invalid end offset.");
        this.asyncInput = asyncInput;
        this.cursorOffset = startOffset;
        this.endOffset = endOffset;

        /*
         * The logic for determining the read length and buffer size are as follows.
         * If we are reading a single event, then we set the read length to be the size
         * of the event plus the header.
         *
         * If this input stream is going to read many events of different sizes, then
         * we set the read length to be equal to the max write size and the buffer
         * size to be twice that. We do it so that we can have at least two events
         * buffered for next event reads.
         */
        this.readLength = Math.min(DEFAULT_READ_LENGTH, readLength);

        this.readState = new ReadState();
    }

    @Override
    public Segment getSegmentId() {
        return asyncInput.getSegmentId();
    }

    @Override
    @Synchronized
    public void close() {
        log.trace("Closing {}", this);
        readState.cancel();
        asyncInput.close();
    }

    @Override
    public boolean isClosed() {
        return asyncInput.isClosed();
    }

    // region Cursor

    @Override
    @Synchronized
    public void setOffset(long offset) {
        log.trace("SetOffset {}", offset);
        Preconditions.checkArgument(offset >= 0);
        Exceptions.checkNotClosed(asyncInput.isClosed(), this);
        if (offset != this.cursorOffset) {
            readState.cancel();
            this.cursorOffset = offset;
        }
    }

    @Override
    @Synchronized
    public long getOffset() {
        return cursorOffset;
    }

    /**
     * Reads the next event from the segment.
     *
     * @return a future containing the event data.
     * @throws EndOfSegmentException if the configured {@code endOffset} is reached.
     */
    @Override
    @Synchronized
    public CompletableFuture<ByteBuffer> readAsync() throws EndOfSegmentException {

        // prepare a promise
        if (cursorOffset >= this.endOffset) {
            log.debug("All events up to the configured end offset:{} have been read", endOffset);
            throw new EndOfSegmentException(END_OFFSET_REACHED);
        }
        CompletableFuture<ByteBuffer> promise = readState.reset(cursorOffset);
        promise.whenComplete(this::readCompleted);

        // kick off or continue processing the request
        issueRequestIfNeeded();

        return promise;
    }

    @Synchronized
    private void readCompleted(ByteBuffer result, Throwable th) {
        if (th != null) {
            // an event could not be read; don't update the cursor position.
            log.trace("Failed to read an event @ " + cursorOffset, th);

            // cancel any outstanding request now to ensure that handleRequest doesn't affect the read state
            if (outstandingRequest != null && !outstandingRequest.isDone()) {
                log.trace("Cancel outstanding read request for segment {}", asyncInput.getSegmentId());
                outstandingRequest.cancel(false);
                outstandingRequest = null;
            }
        }
        if (result != null) {
            // an event was read successfully; update the cursor position.
            cursorOffset = readState.offset;
            log.trace("Read an event @ {}", cursorOffset);
        }
    }

    static class ReadState {
        private final ByteBuffer headerReadingBuffer = ByteBuffer.allocate(WireCommands.TYPE_PLUS_LENGTH_SIZE);
        private CompletableFuture<ByteBuffer> outstandingPromise = CompletableFuture.completedFuture(null);
        private ByteBuffer resultBuffer;
        private long offset;

        /**
         * Gets the next offset for reading purposes.
         */
        public long getOffset() {
            return offset;
        }

        /**
         * Resets the read state in preparation for a new read.
         * @param offset the offset to prepare for.
         */
        public CompletableFuture<ByteBuffer> reset(long offset) {
            if (!this.outstandingPromise.isDone()) {
                this.outstandingPromise.cancel(false);
            }
            this.outstandingPromise = new CompletableFuture<>();
            this.headerReadingBuffer.clear();
            this.resultBuffer = null;
            this.offset = offset;
            return this.outstandingPromise;
        }

        /**
         * Updates the read state based on the given {@link SegmentRead}.
         * Note that the {@code segmentRead} is progressively consumed by successive event reads.
         *
         * @param segmentRead a read response.
         * @return an indicator of whether the {@code segmentRead} is valid for the current state.
         */
        public void update(SegmentRead segmentRead) {
            checkState(!isSuccessful());
            verifyIsAtCorrectOffset(segmentRead);

            if (resultBuffer == null) {
                // consume header data from the SegmentRead data buffer
                if (headerReadingBuffer.hasRemaining()) {
                    assert headerReadingBuffer.limit() == WireCommands.TYPE_PLUS_LENGTH_SIZE;
                    offset += fill(headerReadingBuffer, segmentRead.getData());
                }

                if (!headerReadingBuffer.hasRemaining()) {
                    // header is ready
                    headerReadingBuffer.flip();
                    int type = headerReadingBuffer.getInt();
                    int length = headerReadingBuffer.getInt();
                    if (type != WireCommandType.EVENT.getCode()) {
                        throw new InvalidMessageException("Event was of wrong type: " + type);
                    }
                    if (length < 0 || length > WireCommands.MAX_WIRECOMMAND_SIZE) {
                        throw new InvalidMessageException("Event of invalid length: " + length);
                    }
                    resultBuffer = ByteBuffer.allocate(length);
                }
            }

            if (resultBuffer != null) {
                // consume result data from the SegmentRead data buffer
                if (resultBuffer.hasRemaining()) {
                    offset += fill(resultBuffer, segmentRead.getData());
                }

                if (!resultBuffer.hasRemaining()) {
                    // result is ready
                    resultBuffer.flip();
                    complete();
                }
            }

            if (!isDone() && segmentRead.isEndOfSegment()) {
                // unable to fulfill due to end-of-segment
                assert segmentRead.getData().remaining() == 0;
                endOfSegment();
            }
        }

        private void verifyIsAtCorrectOffset(SegmentRead segmentRead) {
            checkState(isAtCorrectOffset(segmentRead), "ReadSegment returned data for the wrong offset %s vs %s",
                    segmentRead.getOffset() + segmentRead.getData().position(), offset);
        }

        public boolean isAtCorrectOffset(SegmentRead segmentRead) {
            long offsetRead = segmentRead.getOffset() + segmentRead.getData().position();
            long expectedOffset = offset;
            return offsetRead == expectedOffset;
        }

        public boolean isSuccessful() {
            return Futures.isSuccessful(outstandingPromise);
        }

        public boolean isDone() {
            return outstandingPromise.isDone();
        }

        public void completeExceptionally(Throwable th) {
            resultBuffer = null;
            outstandingPromise.completeExceptionally(th);
        }

        public void cancel() {
            resultBuffer = null;
            outstandingPromise.cancel(false);
        }

        private void complete() {
            ByteBuffer buf = resultBuffer;
            resultBuffer = null;
            outstandingPromise.complete(buf);
        }

        private void endOfSegment() {
            completeExceptionally(new EndOfSegmentException());
        }
    }

    // endregion

    // region Request Processing

    @Synchronized
    private void issueRequestIfNeeded() {
        if (outstandingRequest != null
                && Futures.isSuccessful(outstandingRequest)
                && readState.isAtCorrectOffset(outstandingRequest.join())) {
            handleRequest(outstandingRequest.join(), null);
        } else {
            issueRequest(cursorOffset);
        }
    }

    @Synchronized
    private void issueRequest(long offset) {
        assert outstandingRequest == null || outstandingRequest.isDone();
        int updatedReadLength = computeReadLength(offset, readLength);
        log.trace("Issuing request for offset:{}", offset);

        outstandingRequest = asyncInput.read(offset, updatedReadLength);
        outstandingRequest.whenComplete(this::handleRequest);
    }

    /**
     * Compute the read length based on the current fetch offset and the configured end offset.
     */
    private int computeReadLength(long currentFetchOffset, int currentReadLength) {
        Preconditions.checkState(currentFetchOffset < endOffset,
                "Current offset up to which events are fetched should be less than the configured end offset");
        if (UNBOUNDED_END_OFFSET == endOffset) { //endOffset is UNBOUNDED_END_OFFSET if the endOffset is not set.
            return currentReadLength;
        }
        long numberOfBytesRemaining = endOffset - currentFetchOffset;
        return Math.toIntExact(Math.min(currentReadLength, numberOfBytesRemaining));
    }

    /**
     * Process an available {@link SegmentRead} or exception.
     */
    @Synchronized
    private void handleRequest(SegmentRead segmentRead, Throwable th) {
        if (th != null) {
            outstandingRequest = null;
            th = Exceptions.unwrap(th);
            readState.completeExceptionally(th);
            return;
        }
        assert Futures.isSuccessful(outstandingRequest);

        // consume the SegmentRead, which may drive the state to completion
        try {
            readState.update(segmentRead);
        } catch (Exception e) {
            readState.completeExceptionally(e);
        }

        // issue a subsequent request if the read is still incomplete
        if (!readState.isDone()) {
            assert segmentRead.getData().remaining() == 0;
            issueRequest(readState.getOffset());
        } else {
            log.trace("Keeping request:{} at offset:{} for subsequent read", segmentRead.getRequestId(),
                    segmentRead.getOffset() + segmentRead.getData().position());
        }
        assert readState.isDone() || outstandingRequest != null;
    }

    // endregion

    // region Utility

    private static long fill(ByteBuffer to, ByteBuffer from) {
        int toAdd = Math.min(from.remaining(), to.remaining());
        int limit = from.limit();
        from.limit(from.position() + toAdd);
        try {
            to.put(from);
        } finally {
            from.limit(limit);
        }
        return toAdd;
    }

    // endregion
}
