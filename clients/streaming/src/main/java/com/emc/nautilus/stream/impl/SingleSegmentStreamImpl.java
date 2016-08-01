/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.nautilus.stream.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import com.emc.nautilus.stream.Consumer;
import com.emc.nautilus.stream.ConsumerConfig;
import com.emc.nautilus.stream.EventRouter;
import com.emc.nautilus.stream.Position;
import com.emc.nautilus.stream.Producer;
import com.emc.nautilus.stream.ProducerConfig;
import com.emc.nautilus.stream.RateChangeListener;
import com.emc.nautilus.stream.SegmentId;
import com.emc.nautilus.stream.Serializer;
import com.emc.nautilus.stream.Stream;
import com.emc.nautilus.stream.StreamConfiguration;
import com.emc.nautilus.stream.StreamSegments;
import com.emc.nautilus.stream.Transaction.Status;
import com.emc.nautilus.stream.TxFailedException;
import com.emc.nautilus.stream.segment.SegmentManager;
import com.google.common.base.Preconditions;

import lombok.Getter;

public class SingleSegmentStreamImpl implements Stream {

    private final String scope;
    @Getter
    private final String name;
    @Getter
    private final StreamConfiguration config;
    private final SegmentId segmentId;
    private final SegmentManager segmentManager;
    private final EventRouter router = new EventRouter() {
        @Override
        public SegmentId getSegmentForEvent(Stream stream, String routingKey) {
            return segmentId;
        }
    };

    private static final class SingleStreamOrderer<T> implements Orderer<T> {
        @Override
        public SegmentConsumer<T> nextConsumer(Collection<SegmentConsumer<T>> logs) {
            Preconditions.checkState(logs.size() == 1);
            return logs.iterator().next();
        }
    };

    public SingleSegmentStreamImpl(String scope, String name, StreamConfiguration config,
            SegmentManager segmentManager) {
        Preconditions.checkNotNull(segmentManager);
        this.scope = scope;
        this.name = name;
        this.config = config;
        this.segmentManager = segmentManager;
        this.segmentId = new SegmentId(scope, name, 1, 0);
    }

    @Override
    public StreamSegments getSegments(long time) {
        return new StreamSegments(Collections.singletonList(segmentId), time);
    }

    @Override
    public StreamSegments getLatestSegments() {
        return getSegments(System.currentTimeMillis());
    }

    @Override
    public long getRate(long time) {
        return 0;
    }

    @Override
    public <T> Producer<T> createProducer(Serializer<T> s, ProducerConfig config) {
        return new ProducerImpl<>(this, segmentManager, router, s, config);
    }

    @Override
    public <T> Consumer<T> createConsumer(Serializer<T> s, ConsumerConfig config, Position startingPosition,
            RateChangeListener l) {
        return new ConsumerImpl<>(this,
                segmentManager,
                s,
                startingPosition.asImpl(),
                new SingleStreamOrderer<T>(),
                l,
                config);
    }

}
