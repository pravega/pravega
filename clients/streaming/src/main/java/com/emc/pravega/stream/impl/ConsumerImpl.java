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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.emc.pravega.stream.Consumer;
import com.emc.pravega.stream.ConsumerConfig;
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.RateChangeListener;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentManager;

public class ConsumerImpl<Type> implements Consumer<Type> {

    private final Serializer<Type> deserializer;
    private final SegmentManager segmentManager;

    private final Stream stream;
    private final Orderer<Type> orderer;
    private final RateChangeListener rateChangeListener;
    private final ConsumerConfig config;
    private final List<SegmentConsumer<Type>> consumers = new ArrayList<>();
    private final Map<SegmentId, Long> futureOwnedLogs = new HashMap<>();
    private final ControllerApi.Consumer apiConsumer;

    ConsumerImpl(Stream stream, ControllerApi.Consumer apiConsumer, SegmentManager segmentManager, Serializer<Type> deserializer, PositionImpl position,
            Orderer<Type> orderer, RateChangeListener rateChangeListener, ConsumerConfig config) {
        this.deserializer = deserializer;
        this.stream = stream;
        this.segmentManager = segmentManager;
        this.orderer = orderer;
        this.rateChangeListener = rateChangeListener;
        this.config = config;
        this.apiConsumer = apiConsumer;
        setPosition(position);
    }

    @Override
    public Type getNextEvent(long timeout) {
        synchronized (consumers) {
            SegmentConsumer<Type> segment = orderer.nextConsumer(consumers);
            try {
                return segment.getNextEvent(timeout);
            } catch (EndOfSegmentException e) {
                handleEndOfSegment(segment);
                return null;
            }
        }
    }

    /**
     * When a segment ends we can immediately start consuming for any future logs that succeed it. If there are no such
     * segments the rate change listener needs to get involved otherwise the consumer may sit idle.
     */
    private void handleEndOfSegment(SegmentConsumer<Type> oldSegment) {
        consumers.remove(oldSegment);
        SegmentId oldLogId = oldSegment.getSegmentId();
        Optional<SegmentId> replacment = futureOwnedLogs.keySet().stream().filter(l -> l.succeeds(oldLogId)).findAny();
        if (replacment.isPresent()) {
            SegmentId segmentId = replacment.get();
            Long position = futureOwnedLogs.remove(segmentId);
            SegmentInputStream in = segmentManager.openSegmentForReading(segmentId.getQualifiedName(), config.getSegmentConfig());
            in.setOffset(position);
            consumers.add(new SegmentConsumerImpl<>(segmentId, in, deserializer));
            rateChangeListener.rateChanged(stream, false);
        } else {
            rateChangeListener.rateChanged(stream, true);
        }
    }

    @Override
    public Position getPosition() {
        synchronized (consumers) {
            Map<SegmentId, Long> positions = consumers.stream()
                .collect(Collectors.toMap(e -> e.getSegmentId(), e -> e.getOffset()));
            return new PositionImpl(positions, futureOwnedLogs);
        }
    }

    @Override
    public ConsumerConfig getConfig() {
        return config;
    }

    @Override
    public void setPosition(Position state) {
        PositionImpl position = state.asImpl();
        synchronized (consumers) {
            futureOwnedLogs.clear();
            futureOwnedLogs.putAll(position.getFutureOwnedLogs());
            for (SegmentId s : position.getOwnedSegments()) {
                SegmentInputStream in = segmentManager.openSegmentForReading(s.getQualifiedName(), config.getSegmentConfig());
                in.setOffset(position.getOffsetForOwnedLog(s));
                consumers.add(new SegmentConsumerImpl<>(s, in, deserializer));
            }
        }
    }

    @Override
    public void close() {
        synchronized (consumers) {
            for (SegmentConsumer<Type> consumer : consumers) {
                consumer.close();
            }
        }
    }
}
