/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.segment.impl;

import com.google.common.base.Preconditions;
import io.pravega.common.LoggerHelpers;
import io.pravega.shared.protocol.netty.InvalidMessageException;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Parses event sized blobs by reading headers from a @see SegmentInputStream
 */
@Slf4j
@ToString
class EventSegmentReaderImpl implements EventSegmentReader {

    // Flaky network
    private static final long READ_TIMEOUT_MS = 500;

    @GuardedBy("$lock")
    private final ByteBuffer headerReadingBuffer = ByteBuffer.allocate(WireCommands.TYPE_PLUS_LENGTH_SIZE);
    private final SegmentInputStream in;

    EventSegmentReaderImpl(SegmentInputStream input) {
        Preconditions.checkNotNull(input);
        this.in = input;
    }

    @Override
    @Synchronized
    public void setOffset(long offset, boolean resendRequest) {
        in.setOffset(offset, resendRequest);
    }

    @Override
    @Synchronized
    public long getOffset() {
        return in.getOffset();
    }

    /**
     * @see EventSegmentReader#read()
     */
    @Override
    @Synchronized
    public ByteBuffer read(long firstByteTimeoutMillis) throws EndOfSegmentException, SegmentTruncatedException {
        long originalOffset = in.getOffset();
        long traceId = LoggerHelpers.traceEnter(log, "read", in.getSegmentId(), originalOffset, firstByteTimeoutMillis);
        boolean success = false;
        boolean timeout = false;
        try {
            ByteBuffer result = readEvent(firstByteTimeoutMillis);
            success = true;
            return result;
        } catch (TimeoutException e) {
            timeout = true;
            log.warn("Timeout observed while trying to read data from Segment store, the read request will be retransmitted. Details: {}", e.getMessage());
            return null;
        } finally {
            LoggerHelpers.traceLeave(log, "read", traceId, in.getSegmentId(), originalOffset, firstByteTimeoutMillis, success);
            if (!success) {
                if (timeout) {
                    in.setOffset(originalOffset, true);
                } else {
                    in.setOffset(originalOffset);
                }
            }
        }
    }
        
    public ByteBuffer readEvent(long firstByteTimeoutMillis) throws EndOfSegmentException, SegmentTruncatedException, TimeoutException {
        headerReadingBuffer.clear();
        int read = in.read(headerReadingBuffer, firstByteTimeoutMillis);
        if (read == 0) {
            // a resend will not be triggered in-case of a firstByteTimeout.
            return null;
        }
        while (headerReadingBuffer.hasRemaining()) {
            if (in.read(headerReadingBuffer, READ_TIMEOUT_MS) == 0) {
                log.warn("Timeout out while trying to read WireCommand headers during read of segment {} at offset {}",
                        in.getSegmentId(), in.getOffset());
                throw new TimeoutException("Timeout while trying to read WireCommand headers");
            }
        }
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

        if (in.read(result, READ_TIMEOUT_MS) == 0) {
            log.warn("Timeout while trying to read Event data of segment {} at offset {}", in.getSegmentId(), in.getOffset());
            throw new TimeoutException("Timeout while trying to read event data");
        }
        while (result.hasRemaining()) {
            if (in.read(result, READ_TIMEOUT_MS) == 0) {
                log.warn("Timeout while trying to read Event data of segment {} at offset {}", in.getSegmentId(), in.getOffset());
                throw new TimeoutException("Timeout while trying to read event data");
            }
        }
        result.flip();
        return result;
    }

    @Override
    @Synchronized
    public CompletableFuture<?> fillBuffer() {
        return in.fillBuffer();
    }
    
    @Override
    @Synchronized
    public void close() {
        in.close();
    }    

    @Override
    @Synchronized
    public boolean isSegmentReady() {
        int bytesInBuffer = in.bytesInBuffer();
        return bytesInBuffer >= WireCommands.TYPE_PLUS_LENGTH_SIZE || bytesInBuffer < 0;
    }

    @Override
    public Segment getSegmentId() {
        return in.getSegmentId();
    }

}
