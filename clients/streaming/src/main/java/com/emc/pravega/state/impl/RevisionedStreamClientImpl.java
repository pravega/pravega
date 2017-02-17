/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.state.impl;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.state.Revision;
import com.emc.pravega.state.RevisionedStreamClient;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.concurrent.GuardedBy;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RevisionedStreamClientImpl<T> implements RevisionedStreamClient<T> {

    private final Segment segment;
    @GuardedBy("lock")
    private final SegmentInputStream in;
    @GuardedBy("lock")
    private final SegmentOutputStream out;
    private final Serializer<T> serializer;
    private final Object lock = new Object();

    @Override
    public Revision writeConditionally(Revision latestRevision, T value) {
        CompletableFuture<Boolean> wasWritten = new CompletableFuture<>();
        long offset = latestRevision.asImpl().getOffsetInSegment();
        ByteBuffer serialized = serializer.serialize(value);
        int size = serialized.remaining();
        try {
            synchronized (lock) {
                out.conditionalWrite(offset, serialized, wasWritten);
            }
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
        if (FutureHelpers.getAndHandleExceptions(wasWritten, RuntimeException::new)) {
            return new RevisionImpl(segment, getNewOffset(offset, size), 0);
        } else {
            return null;
        }
    }

    private static final long getNewOffset(long initial, int size) {
        return initial + size + WireCommands.TYPE_PLUS_LENGTH_SIZE;
    }

    @Override
    public void writeUnconditionally(T value) {
        CompletableFuture<Boolean> wasWritten = new CompletableFuture<>();
        ByteBuffer serialized = serializer.serialize(value);
        try {
            synchronized (lock) {
                out.write(serialized, wasWritten);
            }
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
        FutureHelpers.getAndHandleExceptions(wasWritten, RuntimeException::new);
    }

    @Override
    public Iterator<Entry<Revision, T>> readFrom(Revision start) {
        synchronized (lock) {
            long startOffset = start.asImpl().getOffsetInSegment();
            long endOffset = in.fetchCurrentStreamLength();
            return new StreamIterator(startOffset, endOffset);
        }
    }
    
    @Override
    public Revision fetchRevision() {
        synchronized (lock) {
            long streamLength = in.fetchCurrentStreamLength();
            return new RevisionImpl(segment, streamLength, 0);
        }
    }

    private class StreamIterator implements Iterator<Entry<Revision, T>> {
        private final AtomicLong offset;
        private final long endOffset;

        StreamIterator(long startingOffset, long endOffset) {
            this.offset = new AtomicLong(startingOffset);
            this.endOffset = endOffset;
        }
        
        @Override
        public boolean hasNext() {
            return offset.get() < endOffset;
        }

        @Override
        public Entry<Revision, T> next() {
            Revision revision;
            ByteBuffer data;
            synchronized (lock) {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                in.setOffset(offset.get());
                try {
                    data = in.read();
                } catch (EndOfSegmentException e) {
                    throw new IllegalStateException(
                            "SegmentInputStream: " + in + " shrunk from its original length: " + endOffset);
                }
                offset.set(in.getOffset());
                revision = new RevisionImpl(segment, offset.get(), 0);
            }
            return new AbstractMap.SimpleImmutableEntry<>(revision, serializer.deserialize(data));
        }
    }
}
