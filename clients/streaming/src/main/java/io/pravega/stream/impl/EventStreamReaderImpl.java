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
package io.pravega.stream.impl;

import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.shared.protocol.netty.WireCommands;
import io.pravega.stream.EventPointer;
import io.pravega.stream.EventRead;
import io.pravega.stream.EventStreamReader;
import io.pravega.stream.ReaderConfig;
import io.pravega.stream.ReinitializationRequiredException;
import io.pravega.stream.Segment;
import io.pravega.stream.Sequence;
import io.pravega.stream.Serializer;
import io.pravega.stream.impl.segment.EndOfSegmentException;
import io.pravega.stream.impl.segment.NoSuchEventException;
import io.pravega.stream.impl.segment.SegmentInputStream;
import io.pravega.stream.impl.segment.SegmentInputStreamFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class EventStreamReaderImpl<Type> implements EventStreamReader<Type> {

    private final Serializer<Type> deserializer;
    private final SegmentInputStreamFactory inputStreamFactory;

    private final Orderer orderer;
    private final ReaderConfig config;
    @GuardedBy("readers")
    private boolean closed;
    @GuardedBy("readers")
    private final List<SegmentInputStream> readers = new ArrayList<>();
    @GuardedBy("readers")
    private Sequence lastRead;
    @GuardedBy("readers")
    private boolean atCheckpoint;
    private final ReaderGroupStateManager groupState;
    private final Supplier<Long> clock;

    EventStreamReaderImpl(SegmentInputStreamFactory inputStreamFactory, Serializer<Type> deserializer, ReaderGroupStateManager groupState,
            Orderer orderer, Supplier<Long> clock, ReaderConfig config) {
        this.deserializer = deserializer;
        this.inputStreamFactory = inputStreamFactory;
        this.groupState = groupState;
        this.orderer = orderer;
        this.clock = clock;
        this.config = config;
        this.closed = false;
    }

    @Override
    public EventRead<Type> readNextEvent(long timeout) throws ReinitializationRequiredException {
        synchronized (readers) {
            Preconditions.checkState(!closed, "Reader is closed");
            long waitTime = Math.min(timeout, ReaderGroupStateManager.TIME_UNIT.toMillis());
            Timer timer = new Timer();
            Segment segment = null;
            long offset = -1;
            ByteBuffer buffer;
            do { 
                String checkpoint = updateGroupStateIfNeeded();
                if (checkpoint != null) {
                     return createEmptyEvent(checkpoint);
                }
                SegmentInputStream segmentReader = orderer.nextSegment(readers);
                if (segmentReader == null) {
                    Exceptions.handleInterrupted(() -> Thread.sleep(waitTime));
                    buffer = null;
                } else {
                    segment = segmentReader.getSegmentId();
                    offset = segmentReader.getOffset();
                    try {
                        buffer = segmentReader.read(waitTime);
                    } catch (EndOfSegmentException e) {
                        handleEndOfSegment(segmentReader);
                        buffer = null;
                    }
                }
            } while (buffer == null && timer.getElapsedMillis() < timeout);
            
            if (buffer == null) {
               return createEmptyEvent(null);
            } 
            lastRead = Sequence.create(segment.getSegmentNumber(), offset);
            int length = buffer.remaining() + WireCommands.TYPE_PLUS_LENGTH_SIZE;
            return new EventReadImpl<>(lastRead,
                    deserializer.deserialize(buffer),
                    getPosition(),
                    new EventPointerImpl(segment, offset, length),
                    null);
        }
    }
    
    private EventRead<Type> createEmptyEvent(String checkpoint) {
        return new EventReadImpl<>(lastRead, null, getPosition(), null, checkpoint);
    }

    private PositionInternal getPosition() {
        Map<Segment, Long> positions = readers.stream()
                .collect(Collectors.toMap(e -> e.getSegmentId(), e -> e.getOffset()));
        return new PositionImpl(positions);
    }
    
    /**
     * Releases or acquires segments as needed. Returns the name of the checkpoint if the reader is
     * at one.
     * 
     * Segments can only be released on the next read call following a checkpoint because this is
     * the only point we can be sure the caller has persisted their position, which is needed to be
     * sure the segment is located in the position of one of the readers and not left out because it
     * was moved while the checkpoint was occurring, while at the same time guaranteeing that
     * another reader will not see events following the ones read by this reader until after they
     * have been persisted.
     */
    @GuardedBy("readers")
    private String updateGroupStateIfNeeded() throws ReinitializationRequiredException {
        try {
            if (atCheckpoint) {
                releaseSegmentsIfNeeded();
                atCheckpoint = false;
            }
            acquireSegmentsIfNeeded();
            String checkpoint = groupState.getCheckpoint();
            if (checkpoint != null) {
                log.info("{} at checkpoint {}", this, checkpoint);
                groupState.checkpoint(checkpoint, getPosition());
                atCheckpoint = true;
            }
            return checkpoint;
        } catch (ReinitializationRequiredException e) {
            close();
            throw e;
        }
    }

    private void releaseSegmentsIfNeeded() throws ReinitializationRequiredException {
        Segment segment = groupState.findSegmentToReleaseIfRequired();
        if (segment != null) {
            log.info("{} releasing segment {}", this, segment);
            SegmentInputStream reader = readers.stream().filter(r -> r.getSegmentId().equals(segment)).findAny().orElse(null);
            if (reader != null) {
                groupState.releaseSegment(segment, reader.getOffset(), getLag());
                readers.remove(reader);
            }
        }
    }

    private void acquireSegmentsIfNeeded() throws ReinitializationRequiredException {
        Map<Segment, Long> newSegments = groupState.acquireNewSegmentsIfNeeded(getLag());
        if (!newSegments.isEmpty()) {
            log.info("{} acquiring segments {}", this, newSegments);
            for (Entry<Segment, Long> newSegment : newSegments.entrySet()) {
                SegmentInputStream in = inputStreamFactory.createInputStreamForSegment(newSegment.getKey());
                in.setOffset(newSegment.getValue());
                readers.add(in);
            }
        }
    }

    //TODO: This is broken until https://github.com/pravega/pravega/issues/191 is implemented.
    private long getLag() {
        if (lastRead == null) {
            return 0;
        }
        return clock.get() - lastRead.getHighOrder();
    }
    
    private void handleEndOfSegment(SegmentInputStream oldSegment) throws ReinitializationRequiredException {
        try {
            log.info("{} encountered end of segment {} ", this, oldSegment.getSegmentId());
            readers.remove(oldSegment);
            groupState.handleEndOfSegment(oldSegment.getSegmentId());
        } catch (ReinitializationRequiredException e) {
            close();
            throw e;
        }
    }

    @Override
    public ReaderConfig getConfig() {
        return config;
    }

    @Override
    public void close() {
        synchronized (readers) {
            if (!closed) {
                log.info("Closing reader {} ", this);
                closed = true;
                groupState.readerShutdown(getPosition());
                for (SegmentInputStream reader : readers) {
                    reader.close();
                }
                readers.clear();
            }
        }
    }

    @Override
    public Type read(EventPointer pointer) throws NoSuchEventException {
        Preconditions.checkNotNull(pointer);
        // Create SegmentInputBuffer
        SegmentInputStream inputStream = inputStreamFactory.createInputStreamForSegment(pointer.asImpl().getSegment(),
                                                                                        pointer.asImpl().getEventLength());
        inputStream.setOffset(pointer.asImpl().getEventStartOffset());
        // Read event
        try {
            ByteBuffer buffer = inputStream.read();
            Type result = deserializer.deserialize(buffer);
            return result;
        } catch (EndOfSegmentException e) {
            throw new NoSuchEventException(e.getMessage());
        }
    }

    @Synchronized
    @VisibleForTesting
    List<SegmentInputStream> getReaders() {
        return Collections.unmodifiableList(readers);
    }

    @Override
    public String toString() {
        return "EventStreamReaderImpl( id=" + groupState.getReaderId() + ")";
    }
    
}
