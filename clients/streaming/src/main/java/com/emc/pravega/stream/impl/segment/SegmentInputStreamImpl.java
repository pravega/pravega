/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl.segment;

import static com.emc.pravega.common.netty.WireCommandType.EVENT;
import static com.emc.pravega.common.netty.WireCommands.MAX_WIRECOMMAND_SIZE;
import static com.emc.pravega.common.netty.WireCommands.TYPE_PLUS_LENGTH_SIZE;
import static com.emc.pravega.stream.impl.segment.SegmentOutputStream.MAX_WRITE_SIZE;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.GuardedBy;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.InvalidMessageException;
import com.emc.pravega.common.netty.WireCommands.SegmentRead;
import com.emc.pravega.common.util.CircularBuffer;
import com.emc.pravega.stream.impl.segment.AsyncSegmentInputStream.ReadFuture;
import com.google.common.base.Preconditions;

import lombok.Synchronized;

/**
 * Manages buffering and provides a synchronus to {@link AsyncSegmentInputStream}
 * 
 * @see SegmentInputStream
 */
class SegmentInputStreamImpl implements SegmentInputStream {
    private static final int DEFAULT_READ_LENGTH = MAX_WRITE_SIZE;
    static final int DEFAULT_BUFFER_SIZE = 2 * SegmentInputStreamImpl.DEFAULT_READ_LENGTH;

    private final AsyncSegmentInputStream asyncInput;
    private final int readLength;
    @GuardedBy("$lock")
    private final CircularBuffer buffer;
    @GuardedBy("$lock")
    private final ByteBuffer headerReadingBuffer = ByteBuffer.allocate(TYPE_PLUS_LENGTH_SIZE);
    @GuardedBy("$lock")
    private long offset;
    @GuardedBy("$lock")
    private boolean receivedEndOfSegment = false;
    @GuardedBy("$lock")
    private ReadFuture outstandingRequest = null;

    SegmentInputStreamImpl(AsyncSegmentInputStream asyncInput, long offset) {
        this(asyncInput, offset, DEFAULT_BUFFER_SIZE);
    }

    SegmentInputStreamImpl(AsyncSegmentInputStream asyncInput, long offset, int bufferSize) {
        Preconditions.checkArgument(offset >= 0);
        Preconditions.checkNotNull(asyncInput);
        this.asyncInput = asyncInput;
        this.offset = offset;
        /*
         * The logic for read length and buffer size is the following. The buffer size needs
         * to be such that it can accommodate all data we read. For example, if we need to read
         * twice to fetch an event, then the buffer size must be at least two times the
         * read length.
         *
         * If we are reading a single event via the read() call of EventStreamReader, then
         * we want to minimize the size of the buffer used to fetch the single event. In this
         * case, we want to set it to be the header size plus the buffer size to hold the event.
         */
        this.readLength = Math.min(DEFAULT_READ_LENGTH, TYPE_PLUS_LENGTH_SIZE  + bufferSize);
        this.buffer = new CircularBuffer(Math.max(bufferSize, readLength + 1));

        issueRequestIfNeeded();
    }

    @Override
    @Synchronized
    public void setOffset(long offset) {
        Preconditions.checkArgument(offset >= 0);
        if (offset != this.offset) {
            this.offset = offset;
            buffer.clear();
            receivedEndOfSegment = false;
            outstandingRequest = null;        
        }
    }

    @Override
    @Synchronized
    public long getOffset() {
        return offset;
    }

    /**
     * @see com.emc.pravega.stream.impl.segment.SegmentInputStream#read()
     */
    @Override
    @Synchronized
    public ByteBuffer read() throws EndOfSegmentException {
        issueRequestIfNeeded();
        while (dataWaitingToGoInBuffer() || (buffer.dataAvailable() < TYPE_PLUS_LENGTH_SIZE)) {
            handleRequest();
        }
        if (buffer.dataAvailable() <= 0 && receivedEndOfSegment) {
            throw new EndOfSegmentException();
        }
        long originalOffset = offset;
        boolean success = false;
        try {
            headerReadingBuffer.clear();
            offset += buffer.read(headerReadingBuffer);
            headerReadingBuffer.flip();
            int type = headerReadingBuffer.getInt();
            int length = headerReadingBuffer.getInt();
            if (type != EVENT.getCode()) {
                throw new InvalidMessageException("Event was of wrong type: " + type);
            }
            if (length < 0 || length > MAX_WIRECOMMAND_SIZE) {
                throw new InvalidMessageException("Event of invalid length: " + length);
            }
            ByteBuffer result = ByteBuffer.allocate(length);
            offset += buffer.read(result);
            while (result.hasRemaining()) {
                handleRequest();
                offset += buffer.read(result);
            }
            success = true;
            result.flip();
            return result;
        } finally {
            if (!success) {
                offset = originalOffset;
                buffer.clear();
            }
        }
    }


    private boolean dataWaitingToGoInBuffer() {
        return outstandingRequest != null && outstandingRequest.isSuccess() && buffer.capacityAvailable() > 0;
    }

    private void handleRequest() {
        SegmentRead segmentRead = asyncInput.getResult(outstandingRequest);
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

    /**
     * @return If there is enough room for another request, and we aren't already waiting on one
     */
    private void issueRequestIfNeeded() {
        if (!receivedEndOfSegment && outstandingRequest == null && buffer.capacityAvailable() > readLength) {
            outstandingRequest = asyncInput.read(offset + buffer.dataAvailable(), readLength);
        }
    }

    @Override
    public long fetchCurrentStreamLength() {
        return FutureHelpers.getAndHandleExceptions(asyncInput.getSegmentInfo(), RuntimeException::new).getSegmentLength();
    }

    @Override
    @Synchronized
    public void close() {
        asyncInput.close();
    }

    @Synchronized
    public boolean canReadWithoutBlocking() {
        while (dataWaitingToGoInBuffer()) {
            handleRequest();
        }
        return buffer.dataAvailable() > 0;
    }

}
