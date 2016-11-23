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
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.netty.ConnectionFactory;
import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Synchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.state.Update;
import com.emc.pravega.state.impl.CorruptedStateException;
import com.emc.pravega.state.impl.SynchronizerImpl;
import com.emc.pravega.stream.Consumer;
import com.emc.pravega.stream.ConsumerConfig;
import com.emc.pravega.stream.EventRouter;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.Producer;
import com.emc.pravega.stream.ProducerConfig;
import com.emc.pravega.stream.RateChangeListener;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentInputStreamFactoryImpl;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactoryImpl;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.util.Collection;

/**
 * An implementation of a stream for the special case where the stream is only ever composed of one segment.
 */
public class StreamImpl implements Stream {

    @Getter
    private final String scope;
    @Getter
    private final String streamName;
    @Getter
    private final StreamConfiguration config;
    private final Controller controller;
    private final ConnectionFactory connectionFactory;

    private final EventRouter router;

    private static final class SingleStreamOrderer<T> implements Orderer<T> {
        @Override
        public SegmentConsumer<T> nextConsumer(Collection<SegmentConsumer<T>> logs) {
            Preconditions.checkState(logs.size() == 1);
            return logs.iterator().next();
        }
    }

    public StreamImpl(String scope, String streamName, StreamConfiguration config, Controller controller, ConnectionFactory connectionFactory) {
        Preconditions.checkNotNull(streamName);
        Preconditions.checkNotNull(controller);
        Preconditions.checkNotNull(connectionFactory);
        this.scope = scope;
        this.streamName = streamName;
        this.config = config;
        this.controller = controller;
        this.connectionFactory = connectionFactory;
        this.router = new EventRouterImpl(this, controller);
    }

    @Override
    public <T> Producer<T> createProducer(Serializer<T> s, ProducerConfig config) {
        return new ProducerImpl<T>(this, controller, new SegmentOutputStreamFactoryImpl(controller, connectionFactory), router, s, config);
    }

    @Override
    public <T> Consumer<T> createConsumer(Serializer<T> s, ConsumerConfig config, Position startingPosition,
                                          RateChangeListener l) {
        return new ConsumerImpl<T>(this,
                new SegmentInputStreamFactoryImpl(controller, connectionFactory),
                s,
                startingPosition.asImpl(),
                new SingleStreamOrderer<T>(),
                l,
                config);
    }

    @Override
    public String getQualifiedName() {
        StringBuffer sb = new StringBuffer();
        if (scope != null) {
            sb.append(scope);
            sb.append('/');
        }
        sb.append(streamName);
        return sb.toString();
    }

    @Override
    public <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>> Synchronizer<StateT, UpdateT, InitT> createSynchronizer(
            Serializer<UpdateT> updateSerializer, Serializer<InitT> initialSerializer, SynchronizerConfig config) {       
        Segment segment = new Segment(scope, streamName, 0);
        SegmentInputStreamFactoryImpl inFactory = new SegmentInputStreamFactoryImpl(controller, connectionFactory);
        SegmentInputStream in = inFactory.createInputStreamForSegment(segment, config.getInputConfig());
        SegmentOutputStreamFactoryImpl outFactory = new SegmentOutputStreamFactoryImpl(controller, connectionFactory);
        SegmentOutputStream out;
        try {
            out = outFactory.createOutputStreamForSegment(segment, config.getOutputConfig());
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Attempted to create synchronizer on sealed segment", e);
        }
        return new SynchronizerImpl<StateT,UpdateT,InitT>(this, in, out, updateSerializer, initialSerializer);
    }
}
