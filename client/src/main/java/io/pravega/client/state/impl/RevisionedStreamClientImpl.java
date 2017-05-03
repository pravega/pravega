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
package io.pravega.client.state.impl;

import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.client.stream.Segment;
import io.pravega.client.stream.Sequence;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.client.stream.impl.segment.EndOfSegmentException;
import io.pravega.client.stream.impl.segment.SegmentInputStream;
import io.pravega.client.stream.impl.segment.SegmentOutputStream;
import io.pravega.client.stream.impl.segment.SegmentSealedException;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class RevisionedStreamClientImpl<T> implements RevisionedStreamClient<T> {

    private final Segment segment;
    @GuardedBy("lock")
    private final SegmentInputStream in;
    @GuardedBy("lock")
    private final SegmentOutputStream out;
    private final Serializer<T> serializer;
    private final AtomicLong sequenceNumber = new AtomicLong(0);
    private final Object lock = new Object();

    @Override
    public Revision writeConditionally(Revision latestRevision, T value) {
        CompletableFuture<Boolean> wasWritten = new CompletableFuture<>();
        long offset = latestRevision.asImpl().getOffsetInSegment();
        ByteBuffer serialized = serializer.serialize(value);
        int size = serialized.remaining();
        try {
            synchronized (lock) {
                Sequence seq = Sequence.create(0, sequenceNumber.incrementAndGet());
                PendingEvent event = new PendingEvent(null, seq, serialized, wasWritten, offset);
                out.write(event);
            }
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
        if (FutureHelpers.getAndHandleExceptions(wasWritten, RuntimeException::new)) {
            long newOffset = getNewOffset(offset, size);
            log.trace("Wrote from {} to {}", offset, newOffset);
            return new RevisionImpl(segment, newOffset, 0);
        } else {
            log.trace("Write failed at offset {}", offset);
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
            log.trace("Unconditionally writing: {}", value);
            synchronized (lock) {
                Sequence seq = Sequence.create(0, sequenceNumber.incrementAndGet());
                PendingEvent event = new PendingEvent(null, seq, serialized, wasWritten);
                out.write(event);
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
            log.trace("Creating iterator from {} until {}", startOffset, endOffset);
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
                log.trace("Iterater reading entry at", offset.get());
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
