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

import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentAttribute;
import io.pravega.client.segment.impl.SegmentInputStream;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.shared.protocol.netty.WireCommands;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;
import lombok.Synchronized;

@RequiredArgsConstructor
public class MockSegmentIoStreams implements SegmentOutputStream, SegmentInputStream, SegmentMetadataClient {

    private final Segment segment;
    @GuardedBy("$lock")
    private int readIndex; 
    @GuardedBy("$lock")
    private int eventsWritten = 0;
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
    }

    @Override
    @Synchronized
    public long getOffset() {
        if (readIndex <= 0) {
            return 0;
        } else if (readIndex >= eventsWritten) {
            return writeOffset;
        }
        return offsetList.get(readIndex);
    }

    @Synchronized
    public long fetchCurrentStreamLength() {
        return writeOffset;
    }

    
    @Override
    public ByteBuffer read() throws EndOfSegmentException {
        return read(Long.MAX_VALUE);
    }
    
    @Override
    @Synchronized
    public ByteBuffer read(long timeout) throws EndOfSegmentException {
        if (readIndex >= eventsWritten) {
            throw new EndOfSegmentException();
        }
        ByteBuffer buffer = dataWritten.get(readIndex);
        readIndex++;
        return buffer.slice();
    }

    @Override
    @Synchronized
    public void write(PendingEvent event) {
        if (event.getExpectedOffset() == null || event.getExpectedOffset() == writeOffset) {
            dataWritten.add(event.getData().slice());
            offsetList.add(writeOffset);
            eventsWritten++;
            writeOffset += event.getData().remaining() + WireCommands.TYPE_PLUS_LENGTH_SIZE;
            event.getAckFuture().complete(true);
        } else {
            event.getAckFuture().complete(false);
        }
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
    public boolean canReadWithoutBlocking() {
        return true;
    }

    @Override
    public Segment getSegmentId() {
        return segment;
    }

    @Override
    public void fillBuffer() {
        //Noting to do.
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

}
