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
import io.pravega.common.LoggerHelpers;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.CircularBuffer;
import io.pravega.shared.protocol.netty.InvalidMessageException;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.shared.protocol.netty.WireCommands.SegmentRead;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import static com.google.common.base.Preconditions.checkState;
import static io.pravega.client.segment.impl.EndOfSegmentException.ErrorType.END_OFFSET_REACHED;

/**
 * Manages buffering and provides a synchronous to {@link AsyncSegmentInputStream}
 * 
 * @see SegmentInputStream
 */
@Slf4j
@ToString
class SegmentInputStreamImpl implements SegmentInputStream {
    static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    private static final int DEFAULT_READ_LENGTH = 256 * 1024;
    private static final long UNBOUNDED_END_OFFSET = Long.MAX_VALUE;

    private final AsyncSegmentInputStream asyncInput;
    private final int minReadLength;
    @GuardedBy("$lock")
    private final CircularBuffer buffer;
    @GuardedBy("$lock")
    private final ByteBuffer headerReadingBuffer = ByteBuffer.allocate(WireCommands.TYPE_PLUS_LENGTH_SIZE);
    @GuardedBy("$lock")
    private long offset;
    @GuardedBy("$lock")
    private final long endOffset;
    @GuardedBy("$lock")
    private boolean receivedEndOfSegment = false;
    @GuardedBy("$lock")
    private boolean receivedTruncated = false;
    @GuardedBy("$lock")
    private CompletableFuture<SegmentRead> outstandingRequest = null;

    SegmentInputStreamImpl(AsyncSegmentInputStream asyncInput, long startOffset) {
        this(asyncInput, startOffset, UNBOUNDED_END_OFFSET, DEFAULT_BUFFER_SIZE);
    }

    SegmentInputStreamImpl(AsyncSegmentInputStream asyncInput, long startOffset, long endOffset, int bufferSize) {
        Preconditions.checkArgument(startOffset >= 0);
        Preconditions.checkNotNull(asyncInput);
        Preconditions.checkNotNull(endOffset, "endOffset");
        Preconditions.checkArgument(endOffset > startOffset + WireCommands.TYPE_PLUS_LENGTH_SIZE,
                "Invalid end offset.");
        this.asyncInput = asyncInput;
        this.offset = startOffset;
        this.endOffset = endOffset;
        // Reads should not be so large they cannot fit into the buffer.
        this.minReadLength = Math.min(DEFAULT_READ_LENGTH, bufferSize);
        this.buffer = new CircularBuffer(bufferSize);
        issueRequestIfNeeded();
    }

    @Override
    @Synchronized
    public void setOffset(long offset) {
        log.trace("SetOffset {}", offset);
        Preconditions.checkArgument(offset >= 0);
        Exceptions.checkNotClosed(asyncInput.isClosed(), this);
        if (offset != this.offset) {
            this.offset = offset;
            buffer.clear();
            receivedEndOfSegment = false;
            receivedTruncated = false;
            outstandingRequest = null;        
        }
    }

    @Override
    @Synchronized
    public long getOffset() {
        return offset;
    }

    /**
     * @see SegmentInputStream#read()
     */
    @Override
    @Synchronized
    public ByteBuffer read(long timeout) throws EndOfSegmentException, SegmentTruncatedException {
        Exceptions.checkNotClosed(asyncInput.isClosed(), this);
        long originalOffset = offset;
        long traceId = LoggerHelpers.traceEnter(log, "read", getSegmentId(), originalOffset, timeout);
        boolean success = false;
        try {
            ByteBuffer result = readEventData(timeout);
            success = true;
            return result;
        } finally {
            LoggerHelpers.traceLeave(log, "read", traceId, getSegmentId(), originalOffset, timeout);
            if (!success) {
                outstandingRequest = null;
                offset = originalOffset;
                buffer.clear();
            }
        }
    }

    private ByteBuffer readEventData(long timeout) throws EndOfSegmentException, SegmentTruncatedException {
        if (this.offset >= this.endOffset) {
            log.debug("All events up to the configured end offset:{} have been read", endOffset);
            throw new EndOfSegmentException(END_OFFSET_REACHED);
        }
        fillBuffer();
        if (receivedTruncated) {
            throw new SegmentTruncatedException();
        }
        while (buffer.dataAvailable() < WireCommands.TYPE_PLUS_LENGTH_SIZE) {
            if (receivedEndOfSegment) {
                throw new EndOfSegmentException();
            }
            Futures.await(outstandingRequest, timeout);
            if (!outstandingRequest.isDone()) {
                return null;
            }
            handleRequest();
        }
        headerReadingBuffer.clear();
        offset += buffer.read(headerReadingBuffer);
        headerReadingBuffer.flip();
        int type = headerReadingBuffer.getInt();
        int length = headerReadingBuffer.getInt();
        if (type != WireCommandType.EVENT.getCode()) {
            throw new InvalidMessageException("Event was of wrong type: " + type);
        }
        if (length < 0 || length > WireCommands.MAX_WIRECOMMAND_SIZE) {
            throw new InvalidMessageException("Event of invalid length: " + length);
        }
        ByteBuffer result = ByteBuffer.allocate(length);
        offset += buffer.read(result);
        while (result.hasRemaining()) {
            issueRequestIfNeeded();
            handleRequest();
            offset += buffer.read(result);
        }
        result.flip();
        return result;
    }

    private boolean dataWaitingToGoInBuffer() {
        return outstandingRequest != null && Futures.isSuccessful(outstandingRequest) && buffer.capacityAvailable() > 0;
    }

    private void handleRequest() throws SegmentTruncatedException {
        SegmentRead segmentRead;
        try {
            segmentRead = outstandingRequest.join();
        } catch (Exception e) {
            outstandingRequest = null;
            if (Exceptions.unwrap(e) instanceof SegmentTruncatedException) {
                receivedTruncated = true;
                throw new SegmentTruncatedException(e);
            }
            throw e;
        }
        verifyIsAtCorrectOffset(segmentRead);
        if (segmentRead.getData().hasRemaining()) {
            buffer.fill(segmentRead.getData());
        }
        if (segmentRead.isEndOfSegment()) {
            receivedEndOfSegment = true;
        }
        if (!segmentRead.getData().hasRemaining()) {
            outstandingRequest = null;
            issueRequestIfNeeded();
        }
    }

    private void verifyIsAtCorrectOffset(WireCommands.SegmentRead segmentRead) {
        long offsetRead = segmentRead.getOffset() + segmentRead.getData().position();
        long expectedOffset = offset + buffer.dataAvailable();
        checkState(offsetRead == expectedOffset, "ReadSegment returned data for the wrong offset %s vs %s", offsetRead,
                   expectedOffset);
    }

    /**
     * Issues a request
     *  - if there is enough room for another request, and we aren't already waiting on one and
     *  - if we have not read up to the configured endOffset.
     */
    private void issueRequestIfNeeded() {
        //compute read length based on current offset up to which the events are read.
        int updatedReadLength = computeReadLength(offset + buffer.dataAvailable());
        if (!receivedEndOfSegment && !receivedTruncated && updatedReadLength > 0 && outstandingRequest == null) {
            log.trace("Issuing read request for segment {} of {} bytes", getSegmentId(), updatedReadLength);
            outstandingRequest = asyncInput.read(offset + buffer.dataAvailable(), updatedReadLength);
        }
    }

    /**
     * Compute the read length based on the current fetch offset and the configured end offset.
     */
    private int computeReadLength(long currentFetchOffset) {
        Preconditions.checkState(endOffset >= currentFetchOffset,
                "Current offset up to to which events are fetched should be less than the configured end offset");
        int currentReadLength = Math.max(minReadLength, buffer.capacityAvailable());
        if (UNBOUNDED_END_OFFSET == endOffset) { //endOffset is UNBOUNDED_END_OFFSET if the endOffset is not set.
            return currentReadLength;
        }
        long numberOfBytesRemaining = endOffset - currentFetchOffset;
        return Math.toIntExact(Math.min(currentReadLength, numberOfBytesRemaining));
    }

    @Override
    @Synchronized
    public void close() {
        log.trace("Closing {}", this);
        if (outstandingRequest != null) {
            log.trace("Cancel outstanding read request for segment {}", asyncInput.getSegmentId());
            outstandingRequest.cancel(true);
        }
        asyncInput.close();
    }

    @Override
    @Synchronized
    public void fillBuffer() {
        log.trace("Filling buffer {}", this);
        Exceptions.checkNotClosed(asyncInput.isClosed(), this);
        try {      
            issueRequestIfNeeded();
            while (dataWaitingToGoInBuffer()) {
                handleRequest();
            }
        } catch (SegmentTruncatedException e) {
            log.warn("Encountered exception filling buffer", e);
        }
    }
    
    @Override
    @Synchronized
    public boolean isSegmentReady() {
        boolean result = receivedEndOfSegment || receivedTruncated || buffer.dataAvailable() > 0 || (outstandingRequest != null && outstandingRequest.isDone());
        log.trace("isSegmentReady {} on segment {} status is {}", result, getSegmentId(), this);
        return result;
    }

    @Override
    public Segment getSegmentId() {
        return asyncInput.getSegmentId();
    }

}
