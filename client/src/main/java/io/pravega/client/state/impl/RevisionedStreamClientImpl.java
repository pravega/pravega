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
import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
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
import io.pravega.client.segment.impl.ServerTimeoutException;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.concurrent.GuardedBy;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static io.pravega.client.segment.impl.SegmentAttribute.NULL_VALUE;
import static io.pravega.client.segment.impl.SegmentAttribute.RevisionStreamClientMark;
import static java.lang.String.format;

@Slf4j
public class RevisionedStreamClientImpl<T> implements RevisionedStreamClient<T> {

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

    private final ClientConfig clientConfig;

    private final Object lock = new Object();

    public RevisionedStreamClientImpl(Segment segment, EventSegmentReader in, SegmentOutputStreamFactory outFactory,
                                      ConditionalOutputStream conditional, SegmentMetadataClient meta,
                                      Serializer<T> serializer, EventWriterConfig config, DelegationTokenProvider tokenProvider ) {
        this(segment, in, outFactory, conditional, meta, serializer, config, tokenProvider, ClientConfig.builder().build());
    }

    public RevisionedStreamClientImpl(Segment segment, EventSegmentReader in, SegmentOutputStreamFactory outFactory,
                                      ConditionalOutputStream conditional, SegmentMetadataClient meta,
                                      Serializer<T> serializer, EventWriterConfig config, DelegationTokenProvider tokenProvider, ClientConfig clientConfig ) {
        this.segment = segment;
        this.in = in;
        this.conditional = conditional;
        this.meta = meta;
        this.serializer = serializer;
        this.out = outFactory.createOutputStreamForSegment(segment, s -> handleSegmentSealed(), config, tokenProvider);
        this.clientConfig = clientConfig;
        this.readTimeout = clientConfig.getConnectTimeoutMilliSec();
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
    public Iterator<Entry<Revision, T>> readFrom(Revision revision) {
        log.trace("Read segment {} from revision {}", segment, revision);
        synchronized (lock) {
            long startOffset = revision.asImpl().getOffsetInSegment();
            SegmentInfo segmentInfo;
            try {
                segmentInfo = Futures.getThrowingExceptionWithTimeout(meta.getSegmentInfo(), clientConfig.getConnectTimeoutMilliSec());
            } catch (TimeoutException e) {
                throw new ServerTimeoutException(format("Timeout occurred while reading the segment Info for segment {} from revision {}", segment, revision));
            }
            long endOffset = segmentInfo.getWriteOffset();
            if (startOffset < segmentInfo.getStartingOffset()) {
                throw new TruncatedDataException(format("Data at the supplied revision {%s} has been truncated. The current segment info is {%s}", revision, segmentInfo));
            }
            if (startOffset == endOffset) {
                log.debug("No new updates to be read from revision {}", revision);
            }
            log.debug("Creating iterator from {} until {} for segment {} ", startOffset, endOffset, segment);
            return new StreamIterator(startOffset, endOffset);
        }
    }

    @Override
    public Iterator<Entry<Revision, T>> readRange(Revision startRevision, Revision endRevision) {
        Preconditions.checkNotNull(startRevision);
        Preconditions.checkNotNull(endRevision);
        log.trace("Read segment {} from revision {} to revision {}", segment, startRevision, endRevision);
        synchronized (lock) {
            long startOffset = startRevision.asImpl().getOffsetInSegment();
            SegmentInfo segmentInfo;
            try {
                segmentInfo = Futures.getThrowingExceptionWithTimeout(meta.getSegmentInfo(), clientConfig.getConnectTimeoutMilliSec());
            } catch (TimeoutException e) {
                throw new ServerTimeoutException(format("Timeout occurred while reading the segment Info for segment {} from revision {} to revision {}", segment, startRevision, endRevision));
            }
            long endOffset = endRevision.asImpl().getOffsetInSegment();
            if (startOffset < segmentInfo.getStartingOffset()) {
                throw new TruncatedDataException(format("Data at the supplied revision {%s} has been truncated. The current segment info is {%s}", startRevision, segmentInfo));
            }
            if (startOffset == endOffset) {
                log.debug("No new updates to be read from revision {}", startOffset);
            }
            if (startOffset > endOffset) {
                throw new IllegalStateException("startOffset: " + startOffset + " is grater than endOffset: " +  endOffset);
            }
            log.debug("Creating iterator from {} until {} for segment {} ", startOffset, endOffset, segment);
            return new StreamIterator(startOffset, endOffset);
        }
    }
    
    @Override
    public Revision fetchLatestRevision() {
        CompletableFuture<Long> streamLength;
        synchronized (lock) {
            streamLength = meta.fetchCurrentSegmentLength();
        }
        return new RevisionImpl(segment, Futures.getThrowingException(streamLength), 0);
        
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
        CompletableFuture<Long> valueF;
        synchronized (lock) {
            valueF = meta.fetchProperty(RevisionStreamClientMark);
        }
        long value = Futures.getThrowingException(valueF);
        return value == NULL_VALUE ? null : new RevisionImpl(segment, value, 0);
    }

    @Override
    public boolean compareAndSetMark(Revision expected, Revision newLocation) {
        long expectedValue = expected == null ? NULL_VALUE : expected.asImpl().getOffsetInSegment();
        long newValue = newLocation == null ? NULL_VALUE : newLocation.asImpl().getOffsetInSegment();
        CompletableFuture<Boolean> result;
        synchronized (lock) {
            result = meta.compareAndSetAttribute(RevisionStreamClientMark, expectedValue, newValue);
        }
        return Futures.getThrowingException(result);
    }

    @Override
    public Revision fetchOldestRevision() {
        long startingOffset = Futures.getThrowingException(meta.getSegmentInfo()).getStartingOffset();
        return new RevisionImpl(segment, startingOffset, 0);
    }

    @Override
    public void truncateToRevision(Revision newStart) {
        Futures.getThrowingException(meta.truncateSegment(newStart.asImpl().getOffsetInSegment()));
        log.info("Truncate segment {} to revision {}", newStart.asImpl().getSegment(), newStart);
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
