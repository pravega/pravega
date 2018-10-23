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
import io.pravega.common.LoggerHelpers;
import io.pravega.shared.protocol.netty.InvalidMessageException;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
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

    @GuardedBy("$lock")
    private final ByteBuffer headerReadingBuffer = ByteBuffer.allocate(WireCommands.TYPE_PLUS_LENGTH_SIZE);
    private final SegmentInputStream in;

    EventSegmentReaderImpl(SegmentInputStream input) {
        Preconditions.checkNotNull(input);
        this.in = input;
    }

    @Override
    @Synchronized
    public void setOffset(long offset) {
        in.setOffset(offset);
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
    public ByteBuffer read(long timeout) throws EndOfSegmentException, SegmentTruncatedException {
        long originalOffset = in.getOffset();
        long traceId = LoggerHelpers.traceEnter(log, "read", getSegmentId(), originalOffset, timeout);
        boolean success = false;
        try {
            ByteBuffer result = readEvent(timeout);
            success = true;
            return result;
        } finally {
            LoggerHelpers.traceLeave(log, "read", traceId, getSegmentId(), originalOffset, timeout);
            if (!success) {
                in.setOffset(originalOffset);
            }
        }
    }
        
    public ByteBuffer readEvent(long timeout) throws EndOfSegmentException, SegmentTruncatedException {
        headerReadingBuffer.clear();
        int read = in.read(headerReadingBuffer, timeout);
        if (read == 0) {
            return null;
        }
        while (headerReadingBuffer.hasRemaining()) {
            in.read(headerReadingBuffer, Long.MAX_VALUE);
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
        in.read(result, Long.MAX_VALUE);
        while (result.hasRemaining()) {
            in.read(result, Long.MAX_VALUE);
        }
        result.flip();
        return result;
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> fillBuffer() {
        return in.fillBuffer();
    }
    
    @Override
    @Synchronized
    public void close() {
        in.close();
    }    

    @Override
    public Segment getSegmentId() {
        return in.getSegmentId();
    }

    @Override
    @Synchronized
    public boolean isSegmentReady() {
        int bytesInBuffer = in.bytesInBuffer();
        return bytesInBuffer >= WireCommands.TYPE_PLUS_LENGTH_SIZE || bytesInBuffer < 0;
    }


}
