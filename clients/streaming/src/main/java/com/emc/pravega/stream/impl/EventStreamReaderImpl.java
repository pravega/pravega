/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.stream.EventPointer;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Sequence;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.NoSuchEventException;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentInputStreamFactory;
import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;


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
    public EventRead<Type> readNextEvent(long timeout) {
        synchronized (readers) {
            Preconditions.checkState(!closed, "Reader is closed");
            long startTime = System.currentTimeMillis();
            Segment segment = null;
            long offset = -1;
            ByteBuffer buffer;
            boolean rebalance = false;
            do { // Loop handles retry on end of segment
                rebalance |= releaseSegmentsIfNeeded();
                rebalance |= acquireSegmentsIfNeeded();
                SegmentInputStream segmentReader = orderer.nextSegment(readers);
                if (segmentReader == null) {
                    Exceptions.handleInterrupted(() -> Thread.sleep(Math.max(timeout, ReaderGroupStateManager.TIME_UNIT.toMillis())));
                    buffer = null;
                } else {
                    segment = segmentReader.getSegmentId();
                    offset = segmentReader.getOffset();
                    try {
                        buffer = segmentReader.read(ReaderGroupStateManager.TIME_UNIT.toMillis());
                    } catch (EndOfSegmentException e) {
                        handleEndOfSegment(segmentReader);
                        buffer = null;
                        rebalance = true;
                    }
                }
            } while (buffer == null && System.currentTimeMillis() < startTime + timeout);
            
            Position position = getPosition();
            if (buffer == null) {
                return new EventReadImpl<>(lastRead, null, position, null, rebalance, null);
            } else {
                lastRead = Sequence.create(segment.getSegmentNumber(), offset);
                int length = buffer.remaining() + WireCommands.TYPE_PLUS_LENGTH_SIZE;
                return new EventReadImpl<>(lastRead,
                        deserializer.deserialize(buffer),
                        position,
                        new EventPointerImpl(segment, offset, length),
                        rebalance,
                        null);
            }
        }
    }

    private PositionInternal getPosition() {
        Map<Segment, Long> positions = readers.stream()
                .collect(Collectors.toMap(e -> e.getSegmentId(), e -> e.getOffset()));
        return new PositionImpl(positions);
    }

    private boolean releaseSegmentsIfNeeded() {
        Segment segment = groupState.findSegmentToReleaseIfRequired();
        if (segment != null) {
            SegmentInputStream reader = readers.stream().filter(r -> r.getSegmentId().equals(segment)).findAny().orElse(null);
            if (reader != null) {
                groupState.releaseSegment(segment, reader.getOffset(), getLag());
                readers.remove(reader);
                return true;
            }
        }
        return false;
    }

    private boolean acquireSegmentsIfNeeded() {
        Map<Segment, Long> newSegments = groupState.acquireNewSegmentsIfNeeded(getLag());
        if (newSegments == null || newSegments.isEmpty()) {
            return false;
        }
        for (Entry<Segment, Long> newSegment : newSegments.entrySet()) {
            SegmentInputStream in = inputStreamFactory.createInputStreamForSegment(newSegment.getKey());
            in.setOffset(newSegment.getValue());
            readers.add(in);
        }
        return true;
    }

    //TODO: This is broken until https://github.com/emccode/pravega/issues/191 is implemented.
    private long getLag() {
        if (lastRead == null) {
            return 0;
        }
        return clock.get() - lastRead.getHighOrder();
    }
    
    private void handleEndOfSegment(SegmentInputStream oldSegment) {
        readers.remove(oldSegment);
        groupState.handleEndOfSegment(oldSegment.getSegmentId());
    }

    @Override
    public ReaderConfig getConfig() {
        return config;
    }

    @Override
    public void close() {
        synchronized (readers) {
            if (!closed) {
                closed = true;
                groupState.readerShutdown(getPosition());
                for (SegmentInputStream reader : readers) {
                    reader.close();
                }
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

}
