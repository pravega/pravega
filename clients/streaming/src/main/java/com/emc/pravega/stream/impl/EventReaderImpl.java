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

import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Sequence;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentInputStreamFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import javax.annotation.concurrent.GuardedBy;

import org.apache.commons.lang.NotImplementedException;

public class EventReaderImpl<Type> implements EventStreamReader<Type> {

    private final Serializer<Type> deserializer;
    private final SegmentInputStreamFactory inputStreamFactory;

    private final Orderer<Type> orderer;
    private final ReaderConfig config;
    @GuardedBy("readers")
    private final List<SegmentReader<Type>> readers = new ArrayList<>();
    @GuardedBy("readers")
    private Sequence lastRead;
    private final ReaderGroupStateManager groupState;

    EventReaderImpl(SegmentInputStreamFactory inputStreamFactory, Serializer<Type> deserializer, ReaderGroupStateManager groupState,
            Orderer<Type> orderer, ReaderConfig config) {
        this.deserializer = deserializer;
        this.inputStreamFactory = inputStreamFactory;
        this.groupState = groupState;
        this.orderer = orderer;
        this.config = config;
    }

    @Override
    public EventRead<Type> readNextEvent(long timeout) {
        synchronized (readers) {
            Segment segmentId;
            long offset;
            Type result;
            boolean rebalance = false;
            do {
                rebalance |= releaseSegmentsIfNeeded();
                rebalance |= aquireSegmentsIfNeeded();
                SegmentReader<Type> segment = orderer.nextSegment(readers);
                segmentId = segment.getSegmentId();
                offset = segment.getOffset();
                try {
                    result = segment.getNextEvent(timeout);
                } catch (EndOfSegmentException e) {
                    handleEndOfSegment(segment);
                    result = null;
                    rebalance = true;
                }
            } while (result == null);
            Map<Segment, Long> positions = readers.stream()
                    .collect(Collectors.toMap(e -> e.getSegmentId(), e -> e.getOffset()));
            Position position = new PositionImpl(positions);
            lastRead = Sequence.create(segmentId.getSegmentNumber(), offset);
            return new EventReadImpl<>(lastRead, result, position, segmentId, offset, rebalance);
        }
    }

    private boolean releaseSegmentsIfNeeded() {
        Segment segment = groupState.shouldReleaseSegment();
        if (segment != null) {
            SegmentReader<Type> reader = readers.stream().filter(r -> r.getSegmentId().equals(segment)).findAny().orElse(null);
            groupState.releaseSegment(segment, reader.getOffset(), lastRead);
            readers.remove(reader);
            return true;
        }
        return false;
    }

    private boolean aquireSegmentsIfNeeded() {
        Map<Segment, Long> newSegments = groupState.aquireNewSegmentsIfNeeded(lastRead);
        if (newSegments == null || newSegments.isEmpty()) {
            return false;
        }
        for (Entry<Segment, Long> newSegment : newSegments.entrySet()) {
            SegmentInputStream in = inputStreamFactory.createInputStreamForSegment(newSegment.getKey(), config.getSegmentConfig());
            in.setOffset(newSegment.getValue());
            readers.add(new SegmentReaderImpl<>(newSegment.getKey(), in, deserializer));            
        }
        return true;
    }
    
    private void handleEndOfSegment(SegmentReader<Type> oldSegment) {
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
            for (SegmentReader<Type> reader : readers) {
                reader.close();
            }
        }
    }

    @Override
    public Type read(Segment segment, long offset) {
        throw new NotImplementedException();
    }

}
