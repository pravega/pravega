/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.emc.pravega.stream.impl;

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
    private final List<SegmentEventReader> readers = new ArrayList<>();
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
    }

    @Override
    public EventRead<Type> readNextEvent(long timeout) {
        synchronized (readers) {
            Segment segment;
            long offset;
            Type result;
            int length = 0;
            boolean rebalance = false;
            do { // Loop handles retry on end of segment
                rebalance |= releaseSegmentsIfNeeded();
                rebalance |= acquireSegmentsIfNeeded();
                SegmentEventReader segmentReader = orderer.nextSegment(readers);
                segment = segmentReader.getSegmentId();
                offset = segmentReader.getOffset();
                try {
                    ByteBuffer buffer = segmentReader.getNextEvent(timeout);
                    result = deserializer.deserialize(buffer);
                    length = buffer.capacity();
                } catch (EndOfSegmentException e) {
                    handleEndOfSegment(segmentReader);
                    result = null;
                    rebalance = true;
                }
            } while (result == null);
            Map<Segment, Long> positions = readers.stream()
                    .collect(Collectors.toMap(e -> e.getSegmentId(), e -> e.getOffset()));
            Position position = new PositionImpl(positions);
            lastRead = Sequence.create(segment.getSegmentNumber(), offset);
            return new EventReadImpl<>(lastRead,
                                        result,
                                        position,
                                        new EventPointerImpl(segment, offset, length),
                                        rebalance);
        }
    }

    private boolean releaseSegmentsIfNeeded() {
        Segment segment = groupState.findSegmentToReleaseIfRequired();
        if (segment != null) {
            SegmentEventReader reader = readers.stream().filter(r -> r.getSegmentId().equals(segment)).findAny().orElse(null);
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
            SegmentInputStream in = inputStreamFactory.createInputStreamForSegment(newSegment.getKey(), config.getSegmentConfig());
            in.setOffset(newSegment.getValue());
            readers.add(new SegmentEventReaderImpl(newSegment.getKey(), in));
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
    
    private void handleEndOfSegment(SegmentEventReader oldSegment) {
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
            for (SegmentEventReader reader : readers) {
                reader.close();
            }
        }
    }

    @Override
    public Type read(EventPointer pointer)
    throws NoSuchEventException {
        // Create SegmentInputBuffer
        SegmentInputStream inputStream = inputStreamFactory.createInputStreamForSegment(pointer.asImpl().getSegment(),
                                                                                        config.getSegmentConfig(),
                                                                                        pointer.asImpl().getEventLength());
        // Create SegmentEventReader and set start offset
        SegmentReader segmentReader = new SegmentReaderImpl(pointer.asImpl().getSegment(), inputStream);
        segmentReader.setOffset(pointer.asImpl().getEventStartOffset());
        // Read event
        try {
            ByteBuffer buffer = segmentReader.getNextEvent(0);
            Type result = deserializer.deserialize(buffer);
            return result;
        } catch (EndOfSegmentException e) {
            throw new NoSuchEventException(e.getMessage());
        }
    }

}
