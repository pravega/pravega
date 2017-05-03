/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.stream.impl.segment;

import io.pravega.client.stream.Segment;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.protocol.netty.InvalidMessageException;
import io.pravega.common.util.CircularBuffer;
import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;

import javax.annotation.concurrent.GuardedBy;

import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import lombok.Synchronized;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages buffering and provides a synchronus to {@link AsyncSegmentInputStream}
 * 
 * @see SegmentInputStream
 */
@Slf4j
@ToString
class SegmentInputStreamImpl implements SegmentInputStream {
    private static final int DEFAULT_READ_LENGTH = 64 * 1024;
    static final int DEFAULT_BUFFER_SIZE = 2 * SegmentInputStreamImpl.DEFAULT_READ_LENGTH;

    private final AsyncSegmentInputStream asyncInput;
    private final int readLength;
    @GuardedBy("$lock")
    private final CircularBuffer buffer;
    @GuardedBy("$lock")
    private final ByteBuffer headerReadingBuffer = ByteBuffer.allocate(WireCommands.TYPE_PLUS_LENGTH_SIZE);
    @GuardedBy("$lock")
    private long offset;
    @GuardedBy("$lock")
    private boolean receivedEndOfSegment = false;
    @GuardedBy("$lock")
    private AsyncSegmentInputStream.ReadFuture outstandingRequest = null;

    SegmentInputStreamImpl(AsyncSegmentInputStream asyncInput, long offset) {
        this(asyncInput, offset, DEFAULT_BUFFER_SIZE);
    }

    SegmentInputStreamImpl(AsyncSegmentInputStream asyncInput, long offset, int bufferSize) {
        Preconditions.checkArgument(offset >= 0);
        Preconditions.checkNotNull(asyncInput);
        this.asyncInput = asyncInput;
        this.offset = offset;
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
        this.readLength = Math.min(DEFAULT_READ_LENGTH, bufferSize);
        this.buffer = new CircularBuffer(Math.max(bufferSize, readLength + 1));

        issueRequestIfNeeded();
    }

    @Override
    @Synchronized
    public void setOffset(long offset) {
        log.trace("SetOffset {}", offset);
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
     * @see SegmentInputStream#read()
     */
    @Override
    @Synchronized
    public ByteBuffer read(long timeout) throws EndOfSegmentException {
        log.trace("Read called at offset {}", offset);
        fillBuffer();
        while (buffer.dataAvailable() < WireCommands.TYPE_PLUS_LENGTH_SIZE) {
            if (buffer.dataAvailable() == 0 && receivedEndOfSegment) {
                throw new EndOfSegmentException();
            }
            if (outstandingRequest.await(timeout)) {
                handleRequest();
            } else {
                return null;
            }
        }
        long originalOffset = offset;
        boolean success = false;
        try {
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
        WireCommands.SegmentRead segmentRead = asyncInput.getResult(outstandingRequest);
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
        log.trace("Fetching current stream length");
        return FutureHelpers.getAndHandleExceptions(asyncInput.getSegmentInfo(), RuntimeException::new).getSegmentLength();
    }

    @Override
    @Synchronized
    public void close() {
        log.trace("Closing {}", this);
        asyncInput.close();
    }

    @Override
    @Synchronized
    public void fillBuffer() {
        log.trace("Filling buffer {}", this);
        issueRequestIfNeeded();
        while (dataWaitingToGoInBuffer()) {
            handleRequest();
        }
    }
    
    @Override
    @Synchronized
    public boolean canReadWithoutBlocking() {
        boolean result = buffer.dataAvailable() > 0 || (outstandingRequest != null && outstandingRequest.isSuccess()
                && asyncInput.getResult(outstandingRequest).getData().hasRemaining());
        log.trace("canReadWithoutBlocking {}", result);
        return result;
    }

    @Override
    public Segment getSegmentId() {
        return asyncInput.getSegmentId();
    }

}
