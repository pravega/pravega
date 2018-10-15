/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.mock;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.ConditionalOutputStream;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentInputStream;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

@RequiredArgsConstructor
public class MockSegmentIoStreams implements SegmentOutputStream, SegmentInputStream, EventSegmentInputStream, ConditionalOutputStream, SegmentMetadataClient {

    private final Segment segment;
    @GuardedBy("$lock")
    private int readIndex; 
    @GuardedBy("$lock")
    private long readOffset = 0;
    @GuardedBy("$lock")
    private int eventsWritten = 0;
    @GuardedBy("$lock")
    private long startingOffset = 0;
    @GuardedBy("$lock")
    private long writeOffset = 0;
    @GuardedBy("$lock")
    private final ArrayList<ByteBuffer> dataWritten = new ArrayList<>();
    @GuardedBy("$lock")
    private final ArrayList<Long> offsetList = new ArrayList<>();
    private final AtomicBoolean close = new AtomicBoolean();
    private final ConcurrentHashMap<SegmentAttribute, Long> attributes = new ConcurrentHashMap<>();
    
    @Override
    @Synchronized
    public void setOffset(long offset) {
        int index = offsetList.indexOf(offset);
        if (index < 0) {
            throw new IllegalArgumentException("There is not an entry at offset: " + offset);
        }
        readIndex = index;
        readOffset = offset;
    }

    @Override
    @Synchronized
    public long getOffset() {
        return readOffset;
    }

    @Override
    @Synchronized
    public long fetchCurrentSegmentLength() {
        return writeOffset;
    }

    
    @Override
    public ByteBuffer read() throws EndOfSegmentException, SegmentTruncatedException {
        return read(Long.MAX_VALUE);
    }
    
    @Override
    @Synchronized
    public ByteBuffer read(long timeout) throws EndOfSegmentException, SegmentTruncatedException {
        if (readIndex >= eventsWritten) {
            throw new EndOfSegmentException();
        }
        if (readOffset < startingOffset) {
            throw new SegmentTruncatedException("Data below " + startingOffset + " has been truncated");
        }
        ByteBuffer buffer = dataWritten.get(readIndex);
        readIndex++;
        readOffset += buffer.remaining();
        ByteBuffer result = buffer.slice();
        result.position(WireCommands.TYPE_PLUS_LENGTH_SIZE);
        return result;
    }

    @Override
    @Synchronized
    public void write(PendingEvent event) {
        ByteBuffer data = event.getData().nioBuffer();
        write(data);
        CompletableFuture<Void> ackFuture = event.getAckFuture();
        if (ackFuture != null) {            
            ackFuture.complete(null);
        }
    }
    
    @Override
    @Synchronized
    public boolean write(ByteBuffer data, long expectedOffset) {
        if (writeOffset == expectedOffset) {
            write(data);
            return true;
        } else {
            return false;
        }
    }

    private void write(ByteBuffer data) {
        dataWritten.add(data.slice());
        offsetList.add(writeOffset);
        eventsWritten++;
        writeOffset += data.remaining();
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
    public boolean isSegmentReady() {
        return true;
    }

    @Override
    public Segment getSegmentId() {
        return segment;
    }

    @Override
    public CompletableFuture<Void> fillBuffer() {
        return CompletableFuture.completedFuture(null);
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
    public long fetchProperty(SegmentAttribute attribute) {
        Long result = attributes.get(attribute);
        return result == null ? SegmentAttribute.NULL_VALUE : result;
    }

    @Override
    public boolean compareAndSetAttribute(SegmentAttribute attribute, long expectedValue, long newValue) {
        attributes.putIfAbsent(attribute, SegmentAttribute.NULL_VALUE);
        return attributes.replace(attribute, expectedValue, newValue);
    }

    public boolean isClosed() {
        return close.get();
    }

    @Override
    @Synchronized
    public SegmentInfo getSegmentInfo() {
        return new SegmentInfo(segment, startingOffset, writeOffset, false, System.currentTimeMillis());
    }

    @Override
    @Synchronized
    public void truncateSegment(Segment segment, long offset) {
        Preconditions.checkArgument(offset <= writeOffset);
        if (offset >= startingOffset) {
            startingOffset = offset;
        }
    }

    @Override
    public String getScopedSegmentName() {
        return segment.getScopedName();
    }

    @Override
    @Synchronized
    public int read(ByteBuffer toFill, long timeout) throws EndOfSegmentException, SegmentTruncatedException {
        if (readIndex >= eventsWritten) {
            throw new EndOfSegmentException();
        }
        if (readOffset < startingOffset) {
            throw new SegmentTruncatedException("Data below " + startingOffset + " has been truncated");
        }
        ByteBuffer buffer = dataWritten.get(readIndex);
        if (buffer.remaining() <= toFill.remaining()) {            
            readIndex++;
        }
        int read = ByteBufferUtils.copy(buffer, toFill);
        readOffset += read;
        return read;
    }

    @Override
    @Synchronized
    public int bytesInBuffer() {
        return (int) (writeOffset - readOffset);
    }
}
