/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.nautilus.streaming.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.emc.nautilus.logclient.EndOfSegmentException;
import com.emc.nautilus.logclient.SegmentManager;
import com.emc.nautilus.logclient.SegmentInputStream;
import com.emc.nautilus.streaming.Consumer;
import com.emc.nautilus.streaming.ConsumerConfig;
import com.emc.nautilus.streaming.Position;
import com.emc.nautilus.streaming.RateChangeListener;
import com.emc.nautilus.streaming.SegmentId;
import com.emc.nautilus.streaming.Serializer;
import com.emc.nautilus.streaming.Stream;

public class ConsumerImpl<Type> implements Consumer<Type> {

    private final Serializer<Type> deserializer;
    private final SegmentManager segmentManager;

    private final Stream stream;
    private final Orderer<Type> orderer;
    private final RateChangeListener rateChangeListener;
    private final ConsumerConfig config;
    private final List<SegmentConsumer<Type>> consumers = new ArrayList<>();
    private final Map<SegmentId, Long> futureOwnedLogs = new HashMap<>();

    ConsumerImpl(Stream stream, SegmentManager segmentManager, Serializer<Type> deserializer, PositionImpl position,
            Orderer<Type> orderer, RateChangeListener rateChangeListener, ConsumerConfig config) {
        this.deserializer = deserializer;
        this.stream = stream;
        this.segmentManager = segmentManager;
        this.orderer = orderer;
        this.rateChangeListener = rateChangeListener;
        this.config = config;
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

    private void handleEndOfSegment(SegmentConsumer<Type> oldSegment) {
        consumers.remove(oldSegment);
        SegmentId oldLogId = oldSegment.getLogId();
        Optional<SegmentId> replacment = futureOwnedLogs.keySet().stream().filter(l -> l.succeeds(oldLogId)).findAny();
        if (replacment.isPresent()) {
            SegmentId segmentId = replacment.get();
            Long position = futureOwnedLogs.remove(segmentId);
            SegmentInputStream in = segmentManager.openLogForReading(segmentId.getQualifiedName(),
                                                                       config.getSegmentConfig());
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
                .collect(Collectors.toMap(e -> e.getLogId(), e -> e.getOffset()));
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
                SegmentInputStream in = segmentManager.openLogForReading(s.getQualifiedName(), config.getSegmentConfig());
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
