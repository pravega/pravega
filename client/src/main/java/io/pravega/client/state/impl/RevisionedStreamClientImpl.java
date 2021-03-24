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
package io.pravega.client.state.impl;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.client.security.auth.DelegationTokenProvider;
import io.pravega.client.segment.impl.ConditionalOutputStream;
import io.pravega.client.segment.impl.EndOfSegmentException;
import io.pravega.client.segment.impl.EventSegmentReader;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.segment.impl.SegmentInfo;
import io.pravega.client.segment.impl.SegmentMetadataClient;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentOutputStreamFactory;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.segment.impl.SegmentTruncatedException;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TruncatedDataException;
import io.pravega.client.stream.impl.PendingEvent;
import io.pravega.common.concurrent.Futures;
import io.pravega.shared.protocol.netty.WireCommands;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.client.segment.impl.SegmentAttribute.NULL_VALUE;
import static io.pravega.client.segment.impl.SegmentAttribute.RevisionStreamClientMark;
import static java.lang.String.format;

@Slf4j
public class RevisionedStreamClientImpl<T> implements RevisionedStreamClient<T> {

    private static final long READ_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(30);
    @Getter
    @VisibleForTesting
    private final long readTimeout;
    private final Segment segment;
    @GuardedBy("lock")
    private final EventSegmentReader in;
    @GuardedBy("lock")
    private final SegmentOutputStream out;
    @GuardedBy("lock")
    private final ConditionalOutputStream conditional;
    @GuardedBy("lock")
    @VisibleForTesting
    @Getter
    private final SegmentMetadataClient meta;
    private final Serializer<T> serializer;

    private final Object lock = new Object();

    public RevisionedStreamClientImpl(Segment segment, EventSegmentReader in, SegmentOutputStreamFactory outFactory,
                                      ConditionalOutputStream conditional, SegmentMetadataClient meta,
                                      Serializer<T> serializer, EventWriterConfig config, DelegationTokenProvider tokenProvider ) {
        this.readTimeout = READ_TIMEOUT_MS;
        this.segment = segment;
        this.in = in;
        this.conditional = conditional;
        this.meta = meta;
        this.serializer = serializer;
        this.out = outFactory.createOutputStreamForSegment(segment, s -> handleSegmentSealed(), config, tokenProvider);
    }


    @Override
    public Revision writeConditionally(Revision latestRevision, T value) {
        boolean wasWritten;
        long offset = latestRevision.asImpl().getOffsetInSegment();
        ByteBuffer serialized = serializer.serialize(value);
        int size = serialized.remaining();
        synchronized (lock) {
            try {
                wasWritten = conditional.write(serialized, offset);
            } catch (SegmentSealedException e) {
                throw new CorruptedStateException("Unexpected end of segment ", e);
            }
        }
        if (wasWritten) {
            long newOffset = getNewOffset(offset, size);
            log.debug("Wrote from {} to {}", offset, newOffset);
            return new RevisionImpl(segment, newOffset, 0);
        } else {
            log.debug("Conditional write failed at offset {}", offset);
            return null;
        }
    }

    private static final long getNewOffset(long initial, int size) {
        return initial + size + WireCommands.TYPE_PLUS_LENGTH_SIZE;
    }

    @Override
    public void writeUnconditionally(T value) {
        CompletableFuture<Void> ack = new CompletableFuture<>();
        ByteBuffer serialized = serializer.serialize(value);
        try {
            PendingEvent event = PendingEvent.withHeader(null, serialized, ack);
            log.trace("Unconditionally writing: {} to segment {}", value, segment);
            synchronized (lock) {
                out.write(event);
                out.flush();
            }
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
        Futures.getAndHandleExceptions(ack, RuntimeException::new);
    }

    @Override
    public Iterator<Entry<Revision, T>> readFrom(Revision start) {
        log.trace("Read segment {} from revision {}", segment, start);
        synchronized (lock) {
            long startOffset = start.asImpl().getOffsetInSegment();
            SegmentInfo segmentInfo = meta.getSegmentInfo();
            long endOffset = segmentInfo.getWriteOffset();
            if (startOffset < segmentInfo.getStartingOffset()) {
                throw new TruncatedDataException(format("Data at the supplied revision {%s} has been truncated. The current segment info is {%s}", start, segmentInfo));
            }
            log.debug("Creating iterator from {} until {} for segment {} ", startOffset, endOffset, segment);
            return new StreamIterator(startOffset, endOffset);
        }
    }
    
    @Override
    public Revision fetchLatestRevision() {
        synchronized (lock) {
            long streamLength = meta.fetchCurrentSegmentLength();
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
                log.trace("Iterator reading entry at {}", offset.get());
                in.setOffset(offset.get());
                try {
                    do {
                        data = in.read(getReadTimeout());
                        if (data == null) {
                            log.warn("Timeout while attempting to read offset:{} on segment:{} where the endOffset is {}", offset, segment, endOffset);
                            in.setOffset(offset.get(), true);
                        }
                    } while (data == null);
                } catch (EndOfSegmentException e) {
                    throw new IllegalStateException("SegmentInputStream: " + in + " shrunk from its original length: " + endOffset);
                } catch (SegmentTruncatedException e) {
                    throw new TruncatedDataException(format("Offset at which truncation observed is {%s}", in.getOffset()), e);
                }
                offset.set(in.getOffset());
                revision = new RevisionImpl(segment, offset.get(), 0);
            }
            return new AbstractMap.SimpleImmutableEntry<>(revision, serializer.deserialize(data));
        }
    }

    @Override
    public Revision getMark() {
        log.trace("Fetching mark for segment {}", segment);
        synchronized (lock) {
            long value = meta.fetchProperty(RevisionStreamClientMark);
            return value == NULL_VALUE ? null : new RevisionImpl(segment, value, 0);
        }
    }

    @Override
    public boolean compareAndSetMark(Revision expected, Revision newLocation) {
        long expectedValue = expected == null ? NULL_VALUE : expected.asImpl().getOffsetInSegment();
        long newValue = newLocation == null ? NULL_VALUE : newLocation.asImpl().getOffsetInSegment();
        synchronized (lock) {
            return meta.compareAndSetAttribute(RevisionStreamClientMark, expectedValue, newValue);
        }
    }

    @Override
    public Revision fetchOldestRevision() {
        long startingOffset = meta.getSegmentInfo().getStartingOffset();
        return new RevisionImpl(segment, startingOffset, 0);
    }

    @Override
    public void truncateToRevision(Revision newStart) {
        meta.truncateSegment(newStart.asImpl().getOffsetInSegment());
    }

    @Override
    public void close() {
        synchronized (lock) {
            try {
                out.close();
            } catch (SegmentSealedException e) {
                log.warn("Error closing segment writer {}", out);
            }
            conditional.close();
            meta.close();
            in.close();
        }
    }

    @VisibleForTesting
    void handleSegmentSealed() {
        log.debug("Complete all unacked events with SegmentSealedException for segment {}", segment);
        List<PendingEvent> r = out.getUnackedEventsOnSeal();
        r.stream()
         .filter(pendingEvent -> pendingEvent.getAckFuture() != null)
         .forEach(pendingEvent -> pendingEvent.getAckFuture().completeExceptionally(new SegmentSealedException(segment.toString())));
    }
}
