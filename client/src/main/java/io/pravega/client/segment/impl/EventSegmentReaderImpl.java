/**
 * Copyright Pravega Authors.
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
package io.pravega.client.segment.impl;

import com.google.common.base.Preconditions;
import io.pravega.common.LoggerHelpers;
import io.pravega.shared.protocol.netty.InvalidMessageException;
import io.pravega.shared.protocol.netty.WireCommandType;
import io.pravega.shared.protocol.netty.WireCommands;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.concurrent.GuardedBy;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Synchronized;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * Parses event sized blobs by reading headers from a @see SegmentInputStream
 */
@Slf4j
@ToString
class EventSegmentReaderImpl implements EventSegmentReader {

    /*
     * This timeout is the maximum amount of time the reader will wait in the case of partial event data being received
     *  by the client. After this timeout the client will resend the request.
     */
    static final long PARTIAL_DATA_TIMEOUT = TimeUnit.SECONDS.toMillis(30);

    @GuardedBy("$lock")
    private final ByteBuffer headerReadingBuffer = ByteBuffer.allocate(WireCommands.TYPE_PLUS_LENGTH_SIZE);
    @Getter(value = AccessLevel.MODULE)
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
            log.warn("Timeout observed while trying to read data from Segment store, the read request will be retransmitted");
            return null;
        } finally {
            LoggerHelpers.traceLeave(log, "read", traceId, in.getSegmentId(), originalOffset, firstByteTimeoutMillis, success);
            if (!success) {
                // Reading failed, reset the offset to the original offset.
                // The read request is retransmitted only in the case of a timeout.
                in.setOffset(originalOffset, timeout);
            }
        }
    }
        
    private ByteBuffer readEvent(long firstByteTimeoutMillis) throws EndOfSegmentException, SegmentTruncatedException, TimeoutException {
        headerReadingBuffer.clear();
        int read = in.read(headerReadingBuffer, firstByteTimeoutMillis);
        if (read == 0) {
            log.debug("Empty read for segment id {}.", in.getSegmentId());
            // a resend will not be triggered in-case of a firstByteTimeout.
            return null;
        }
        while (headerReadingBuffer.hasRemaining()) {
            readEventDataFromSegmentInputStream(headerReadingBuffer);
        }
        headerReadingBuffer.flip();
        int type = headerReadingBuffer.getInt();
        int length = headerReadingBuffer.getInt();
        if (type != WireCommandType.EVENT.getCode()) {
            throw new InvalidMessageException("Event was of wrong type: " + type);
        }
        if (length < 0) {
            throw new InvalidMessageException("Event of invalid length: " + length);
        }
        ByteBuffer result = ByteBuffer.allocate(length);

        readEventDataFromSegmentInputStream(result);
        while (result.hasRemaining()) {
            readEventDataFromSegmentInputStream(result);
        }
        result.flip();
        return result;
    }

    private void readEventDataFromSegmentInputStream(ByteBuffer result) throws EndOfSegmentException, SegmentTruncatedException, TimeoutException {
        //SSS can return empty events incase of @link{io.pravega.segmentstore.server.host.handler.PravegaRequestProcessor.ReadCancellationException}
        if (in.read(result, PARTIAL_DATA_TIMEOUT) == 0 && result.limit() != 0) {
            log.warn("Timeout while trying to read Event data from segment {} at offset {}. The buffer capacity is {} bytes and the data read so far is {} bytes",
                    in.getSegmentId(), in.getOffset(), result.limit(), result.position());
            throw new TimeoutException("Timeout while trying to read event data");
        }
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
