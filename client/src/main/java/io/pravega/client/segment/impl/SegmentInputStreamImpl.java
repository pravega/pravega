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
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.CircularBuffer;
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
    static final int MIN_BUFFER_SIZE = 1024;
    static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
    static final int MAX_BUFFER_SIZE = 10 * 1024 * 1024;
    private static final int DEFAULT_READ_LENGTH = 256 * 1024;
    private static final long UNBOUNDED_END_OFFSET = Long.MAX_VALUE;

    private final AsyncSegmentInputStream asyncInput;
    private final int minReadLength;
    @GuardedBy("$lock")
    private final CircularBuffer buffer;
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
        Preconditions.checkArgument(endOffset >= startOffset, "Invalid end offset.");
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
    public void setOffset(long offset, boolean resendRequest) {
        log.trace("SetOffset {}", offset);
        Preconditions.checkArgument(offset >= 0);
        Exceptions.checkNotClosed(asyncInput.isClosed(), this);
        if (offset > this.offset) {
            receivedTruncated = false;
        }
        if (offset != this.offset || resendRequest) {
            if (outstandingRequest != null) {
                log.debug("Cancelling the read request for segment {} at offset {}. The new read offset is {}", asyncInput.getSegmentId(), this.offset, offset);
                cancelOutstandingRequest();
            }
            this.offset = offset;
            buffer.clear();
            receivedEndOfSegment = false;
        }
    }

    @Override
    @Synchronized
    public long getOffset() {
        return offset;
    }

    /**
     * @see SegmentInputStream#read(ByteBuffer, long)
     */
    @Override
    @Synchronized
    public int read(ByteBuffer toFill, long timeout) throws EndOfSegmentException, SegmentTruncatedException {
        Exceptions.checkNotClosed(asyncInput.isClosed(), this);
        if (this.offset >= this.endOffset) {
            log.debug("All events up to the configured end offset:{} have been read", endOffset);
            throw new EndOfSegmentException(END_OFFSET_REACHED);
        }
        if (outstandingRequest == null) {
            fillBuffer();
        }
        if (receivedTruncated) {
            throw new SegmentTruncatedException();
        }
        while (buffer.dataAvailable() == 0) {
            if (receivedEndOfSegment) {
                throw new EndOfSegmentException();
            }
            Futures.await(outstandingRequest, timeout);
            if (!outstandingRequest.isDone()) {
                return 0;
            }
            handleRequest();
        }
        
        int read = buffer.read(toFill);
        offset += read;
        return read;
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
        if (segmentRead.getData().readableBytes() > 0) {
            int copied = buffer.fill(segmentRead.getData().nioBuffers());
            segmentRead.getData().skipBytes(copied);
        }
        if (segmentRead.isEndOfSegment()) {
            receivedEndOfSegment = true;
        }
        if (segmentRead.getData().readableBytes() == 0) {
            segmentRead.release();
            outstandingRequest = null;
            issueRequestIfNeeded();
        }
    }

    private void verifyIsAtCorrectOffset(WireCommands.SegmentRead segmentRead) {
        long offsetRead = segmentRead.getOffset() + segmentRead.getData().readerIndex();
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
            if (log.isTraceEnabled()) {
                log.trace("Issuing read request for segment {} of {} bytes", getSegmentId(), updatedReadLength);
            }
            CompletableFuture<SegmentRead> r = asyncInput.read(offset + buffer.dataAvailable(), updatedReadLength);
            outstandingRequest = Futures.cancellableFuture(r, SegmentRead::release);
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

    @GuardedBy("$lock")
    private void cancelOutstandingRequest() {
        // We need to make sure that we release the ByteBuf held on to by WireCommands.SegmentRead.
        // We first attempt to cancel the request. If it has not already completed (and will complete successfully at one point),
        // it will automatically release the buffer.
        outstandingRequest.cancel(true);

        // If the request has already completed successfully, attempt to release it anyway. Doing so multiple times will
        // have no adverse effect. We do this after attempting to cancel (as opposed to before) since the request may very
        // well complete while we're executing this method and we want to ensure no SegmentRead instances are left hanging.
        if (outstandingRequest.isDone() && !outstandingRequest.isCompletedExceptionally()) {
            SegmentRead request = outstandingRequest.join();
            request.release();
        }

        log.debug("Completed cancelling outstanding read request for segment {}", asyncInput.getSegmentId());
        outstandingRequest = null;
    }

    @Override
    @Synchronized
    public void close() {
        log.trace("Closing {}", this);
        if (outstandingRequest != null) {
            log.debug("Cancel outstanding read request for segment {}", asyncInput.getSegmentId());
            cancelOutstandingRequest();
        }
        asyncInput.close();
    }

    @Override
    @Synchronized
    public CompletableFuture<?> fillBuffer() {
        log.trace("Filling buffer {}", this);
        Exceptions.checkNotClosed(asyncInput.isClosed(), this);
        try {
            issueRequestIfNeeded();
            while (dataWaitingToGoInBuffer()) {
                handleRequest();
            }
        } catch (SegmentTruncatedException e) {
            log.warn("Encountered exception filling buffer", e);
            return CompletableFuture.completedFuture(null);
        }
        return outstandingRequest == null ? CompletableFuture.completedFuture(null) : outstandingRequest;
    }
    
    @Override
    @Synchronized
    public int bytesInBuffer() {
        int result = buffer.dataAvailable();
        boolean atEnd = receivedEndOfSegment || receivedTruncated || (outstandingRequest != null && outstandingRequest.isCompletedExceptionally());
        if (outstandingRequest != null && Futures.isSuccessful(outstandingRequest)) {
            SegmentRead request = outstandingRequest.join();
            result += request.getData().readableBytes();
            atEnd |= request.isEndOfSegment();
        }
        if (result <= 0 && atEnd) {
           result = -1;
        }
        log.trace("bytesInBuffer {} on segment {} status is {}", result, getSegmentId(), this);        
        return result;
    }

    @Override
    public Segment getSegmentId() {
        return asyncInput.getSegmentId();
    }
    
    @Synchronized
    int getBufferSize() {
        return buffer.getCapacity();
    }

}
