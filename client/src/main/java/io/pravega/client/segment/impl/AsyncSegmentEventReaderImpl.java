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

import com.google.common.annotations.VisibleForTesting;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@Slf4j
@ToString
public class AsyncSegmentEventReaderImpl implements AsyncSegmentEventReader {
    static final int DEFAULT_READ_LENGTH = 2 * 1024 * 1024;

    private final AsyncSegmentInputStream asyncInput;
    private final int readLength;

    @GuardedBy("lock")
    private CompletableFuture<ByteBuffer> outstandingPromise = CompletableFuture.completedFuture(null);
    @GuardedBy("lock")
    private final ReadState readState;
    @GuardedBy("lock")
    private CompletableFuture<SegmentRead> outstandingRequest = null;

    private final Object lock = new Object();

    AsyncSegmentEventReaderImpl(AsyncSegmentInputStream asyncInput) {
        this(asyncInput, DEFAULT_READ_LENGTH);
    }

    AsyncSegmentEventReaderImpl(AsyncSegmentInputStream asyncInput, int readLength) {
        Preconditions.checkNotNull(asyncInput);
        this.asyncInput = asyncInput;
        this.readLength = readLength;
        this.readState = new ReadState();
    }

    @Override
    public Segment getSegmentId() {
        return asyncInput.getSegmentId();
    }

    @Override
    @Synchronized("lock")
    public void close() {
        log.trace("Closing {}", this);
        outstandingPromise.cancel(false);
        readState.clear();
        asyncInput.close();
    }

    @Override
    public boolean isClosed() {
        return asyncInput.isClosed();
    }

    // region Cursor

    @Override
    public CompletableFuture<ByteBuffer> readAsync(long offset) {
        CompletableFuture<ByteBuffer> promise;
        synchronized (lock) {
            // reset the promise
            if (!this.outstandingPromise.isDone()) {
                this.outstandingPromise.cancel(false);
            }
            assert outstandingRequest == null || outstandingRequest.isDone();
            this.outstandingPromise = promise = new CompletableFuture<>();
            promise.whenComplete(this::readCompleted);

            // reset the read state
            readState.reset(offset);

            // kick off or continue processing the request
            issueRequestIfNeeded();
        }
        return promise;
    }

    private void readCompleted(ByteBuffer result, Throwable th) {
        if (th instanceof CancellationException) {
            // received read cancellation signal
            synchronized (lock) {
                cancelRequest();
            }
        }
    }

    @VisibleForTesting
    ReadState getReadState() {
        return readState;
    }

    @VisibleForTesting
    class ReadState {
        private final ByteBuffer headerReadingBuffer = ByteBuffer.allocate(WireCommands.TYPE_PLUS_LENGTH_SIZE);
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
        @GuardedBy("AsyncSegmentEventReaderImpl.lock")
        public void reset(long offset) {
            this.headerReadingBuffer.clear();
            this.resultBuffer = null;
            this.offset = offset;
        }

        /**
         * Updates the read state based on the given {@link SegmentRead}.
         * Note that the {@code segmentRead} is progressively consumed by successive event reads.
         *
         * @param segmentRead a read response.
         * @return buffer if the read has completed.
         */
        @GuardedBy("AsyncSegmentEventReaderImpl.lock")
        public ByteBuffer update(SegmentRead segmentRead) {
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
                    ByteBuffer buf = resultBuffer;
                    buf.flip();
                    clear();
                    return buf;
                }
            }

            return null;
        }

        private void verifyIsAtCorrectOffset(SegmentRead segmentRead) {
            checkState(isAtCorrectOffset(segmentRead), "ReadSegment returned data for the wrong offset %s vs %s",
                    segmentRead.getOffset() + segmentRead.getData().position(), offset);
        }

        @GuardedBy("AsyncSegmentEventReaderImpl.lock")
        public boolean isAtCorrectOffset(SegmentRead segmentRead) {
            long offsetRead = segmentRead.getOffset() + segmentRead.getData().position();
            long expectedOffset = offset;
            return offsetRead == expectedOffset;
        }

        @GuardedBy("AsyncSegmentEventReaderImpl.lock")
        public void clear() {
            resultBuffer = null;
        }
    }

    // endregion

    // region Request Processing

    @GuardedBy("lock")
    private void issueRequestIfNeeded() {
        RequestConsumer consumer = new RequestConsumer(outstandingPromise);
        if (outstandingRequest != null
                && Futures.isSuccessful(outstandingRequest)
                && readState.isAtCorrectOffset(outstandingRequest.join())) {
            consumer.accept(outstandingRequest.join(), null);
        } else {
            issueRequest(readState.getOffset(), consumer);
        }
    }

    @GuardedBy("lock")
    private void issueRequest(long offset, RequestConsumer consumer) {
        assert outstandingRequest == null || outstandingRequest.isDone();
        log.trace("Issuing request for offset:{}", offset);

        outstandingRequest = asyncInput.read(offset, readLength);
        outstandingRequest.whenComplete(consumer);
    }

    @GuardedBy("lock")
    private void cancelRequest() {
        if (outstandingRequest != null) {
            log.trace("Cancel outstanding read request for segment {}", asyncInput.getSegmentId());
            outstandingRequest.cancel(false);
            outstandingRequest = null;
        }
    }

    private class RequestConsumer implements BiConsumer<SegmentRead, Throwable> {
        private final CompletableFuture<ByteBuffer> promise;

        public RequestConsumer(CompletableFuture<ByteBuffer> promise) {
            this.promise = promise;
        }

        /**
         * Process an available {@link SegmentRead} or exception.
         */
        @Override
        public void accept(SegmentRead segmentRead, Throwable th) {
            ByteBuffer buffer = null;
            synchronized (lock) {
                if (promise.isDone()) {
                    // the promise for which this request was issued is already done.
                    // note that readState may already be reset to a subsequent read.
                    log.debug("Discarded a segment read for segment " + getSegmentId());
                    return;
                }

                // update the read state
                if (th != null) {
                    th = Exceptions.unwrap(th);
                    log.warn("Read request failed for segment " + getSegmentId() + " @ " + readState.getOffset(), th);
                } else {
                    checkNotNull(segmentRead);
                    try {
                        buffer = readState.update(segmentRead);
                    } catch (Exception e) {
                        // invalid state
                        th = e;
                    }

                    if (buffer == null && segmentRead.isEndOfSegment()) {
                        // unable to fulfill due to end-of-segment
                        assert segmentRead.getData().remaining() == 0;
                        th = new EndOfSegmentException();
                    }
                }
                if (th != null) {
                    // clear the read state on error (to eagerly release any buffers)
                    readState.clear();
                }

                // issue a subsequent request if the promise is still incomplete
                if (buffer == null && th == null) {
                    assert segmentRead.getData().remaining() == 0;
                    issueRequest(readState.getOffset(), this);
                }
            }

            // complete the promise outside the lock (if necessary)
            if (th != null) {
                promise.completeExceptionally(th);
            } else if (buffer != null) {
                promise.complete(buffer);
            }
        }
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
