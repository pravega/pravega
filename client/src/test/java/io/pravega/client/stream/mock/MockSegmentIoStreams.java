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
package io.pravega.client.stream.mock;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.ConditionalOutputStream;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentAttribute;
import io.pravega.client.segment.impl.SegmentInfo;
import io.pravega.client.segment.impl.SegmentInputStream;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.common.util.ByteBufferUtils;
import io.pravega.shared.protocol.netty.WireCommands;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

@RequiredArgsConstructor
public class MockSegmentIoStreams implements SegmentOutputStream, SegmentInputStream, EventSegmentReader, ConditionalOutputStream, SegmentMetadataClient {

    private final Segment segment;
    @GuardedBy("$lock")
    private long readOffset = 0;
    @GuardedBy("$lock")
    private int eventsWritten = 0;
    @GuardedBy("$lock")
    private long startingOffset = 0;
    @GuardedBy("$lock")
    private long writeOffset = 0;
    @GuardedBy("$lock")
    private final TreeMap<Long, ByteBuffer> dataWritten = new TreeMap<>();
    @GuardedBy("$lock")
    private final AtomicBoolean close = new AtomicBoolean();
    private final ConcurrentHashMap<SegmentAttribute, Long> attributes = new ConcurrentHashMap<>();
    private final Semaphore fillCalled;
    
    @Override
    @Synchronized
    public void setOffset(long offset, boolean resend) {
        if (offset < 0) {
            throw new IllegalArgumentException("Invalid offset " + offset);
        }
        if (offset > writeOffset) {
            throw new IllegalArgumentException("Beyond the end of the stream: " + offset);
        }
        readOffset = offset;
    }

    @Override
    public void setOffset(long offset) {
        setOffset(offset, false);
    }

    @Override
    @Synchronized
    public long getOffset() {
        return readOffset;
    }

    @Override
    @Synchronized
    public CompletableFuture<Long> fetchCurrentSegmentHeadOffset() {
        return CompletableFuture.completedFuture(startingOffset);
    }

    @Override
    @Synchronized
    public CompletableFuture<Long> fetchCurrentSegmentLength() {
        return CompletableFuture.completedFuture(writeOffset);
    }

    
    @Override
    public ByteBuffer read() throws EndOfSegmentException, SegmentTruncatedException {
        return read(Long.MAX_VALUE);
    }
    
    /** 
     * Event read.
     * @see io.pravega.client.segment.impl.EventSegmentReader#read(long)
     */
    @Override
    @Synchronized
    public ByteBuffer read(long timeout) throws EndOfSegmentException, SegmentTruncatedException {
        if (readOffset >= writeOffset) {
            throw new EndOfSegmentException();
        }
        if (readOffset < startingOffset) {
            throw new SegmentTruncatedException("Data below " + startingOffset + " has been truncated");
        }
        ByteBuffer buffer = dataWritten.floorEntry(readOffset).getValue();
        readOffset += buffer.remaining();
        ByteBuffer result = buffer.slice();
        result.position(WireCommands.TYPE_PLUS_LENGTH_SIZE);
        return result;
    }
    
    /** 
     * Byte oriented read.
     * @see io.pravega.client.segment.impl.SegmentInputStream#read(java.nio.ByteBuffer, long)
     */
    @Override
    @Synchronized
    public int read(ByteBuffer toFill, long timeout) throws EndOfSegmentException, SegmentTruncatedException {
        if (readOffset >= writeOffset) {
            throw new EndOfSegmentException();
        }
        if (readOffset < startingOffset) {
            throw new SegmentTruncatedException("Data below " + startingOffset + " has been truncated");
        }
        int result = 0;
        while (toFill.hasRemaining() && readOffset < writeOffset) {
            ByteBuffer buffer = dataWritten.floorEntry(readOffset).getValue();
            int read = ByteBufferUtils.copy(buffer, toFill);
            readOffset += read;
            result += read;
        }
        return result;
    }

    @Override
    public void write(PendingEvent event) {
        CompletableFuture<Void> ackFuture = doWrite(event);
        if (ackFuture != null) {            
            ackFuture.complete(null);
        }
    }

    @Override
    @Synchronized
    public boolean write(ByteBuffer data, long expectedOffset) {
        if (writeOffset == expectedOffset) {
            write(PendingEvent.withHeader(null, data, null).getData().nioBuffer());
            return true;
        } else {
            return false;
        }
    }

    private void write(ByteBuffer data) {
        dataWritten.put(writeOffset, data.slice());
        eventsWritten++;
        writeOffset += data.remaining();
    }
    
    @Synchronized
    private CompletableFuture<Void> doWrite(PendingEvent event) {
        ByteBuffer data = event.getData().nioBuffer();
        write(data);
        return event.getAckFuture();
    }
    
    @Override
    public void close() {
        close.set(true);
    }

    @Override
    public void flush() throws SegmentSealedException {
        //Noting to do.
    }

    @Override
    public void flushAsync() {
        //Noting to do.
    }

    @Override
    public boolean isSegmentReady() {
        return true;
    }

    @Override
    public Segment getSegmentId() {
        return segment;
    }

    @Override
    public CompletableFuture<?> fillBuffer() {
        if (fillCalled != null) {
            fillCalled.release();
        }
        return null;
    }

    @Override
    public List<PendingEvent> getUnackedEventsOnSeal() {
        return Collections.emptyList();
    }

    @Override
    public String getSegmentName() {
        return segment.getScopedName();
    }

    @Override
    public CompletableFuture<Long> fetchProperty(SegmentAttribute attribute) {
        Long result = attributes.get(attribute);
        return CompletableFuture.completedFuture(result == null ? Long.valueOf(SegmentAttribute.NULL_VALUE) : result);
    }

    @Override
    public CompletableFuture<Boolean> compareAndSetAttribute(SegmentAttribute attribute, long expectedValue, long newValue) {
        attributes.putIfAbsent(attribute, SegmentAttribute.NULL_VALUE);
        return CompletableFuture.completedFuture(attributes.replace(attribute, expectedValue, newValue));
    }

    public boolean isClosed() {
        return close.get();
    }

    @Override
    @Synchronized
    public CompletableFuture<SegmentInfo> getSegmentInfo() {
        return CompletableFuture.completedFuture(new SegmentInfo(segment, startingOffset, writeOffset, false, System.currentTimeMillis()));
    }

    @Override
    @Synchronized
    public CompletableFuture<Void> truncateSegment(long offset) {
        Preconditions.checkArgument(offset <= writeOffset);
        if (offset >= startingOffset) {
            startingOffset = offset;
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public String getScopedSegmentName() {
        return segment.getScopedName();
    }

    @Override
    @Synchronized
    public int bytesInBuffer() {
        return (int) (writeOffset - readOffset);
    }

    @Override
    public CompletableFuture<Void> sealSegment() {
        //Nothing to do
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public long getLastObservedWriteOffset() {
        return fetchCurrentSegmentLength().join();
    }
}
