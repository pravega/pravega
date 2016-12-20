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
package com.emc.pravega.stream.impl;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;
import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Synchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.state.Update;
import com.emc.pravega.state.impl.CorruptedStateException;
import com.emc.pravega.state.impl.SynchronizerImpl;
import com.emc.pravega.stream.Consumer;
import com.emc.pravega.stream.ConsumerConfig;
import com.emc.pravega.stream.IdempotentProducer;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.Producer;
import com.emc.pravega.stream.ProducerConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.impl.netty.ConnectionFactory;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentInputStreamFactoryImpl;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStreamFactoryImpl;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.net.URI;
import java.util.Collection;

import org.apache.commons.lang.NotImplementedException;

public class ClientFactoryImpl implements ClientFactory {

    private final String scope;
    private final Controller controller;
    private final ConnectionFactory connectionFactory;
    private final StreamManager streamManager;

    public ClientFactoryImpl(String scope, URI controllerUri) {
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(controllerUri);
        this.scope = scope;
        this.controller = new ControllerImpl(controllerUri.getHost(), controllerUri.getPort());
        this.connectionFactory = new ConnectionFactoryImpl(false);
        this.streamManager = StreamManager.withScope(scope, controllerUri);
    }

    @VisibleForTesting
    public ClientFactoryImpl(String scope, Controller controller, ConnectionFactory connectionFactory,
            StreamManager streamManager) {
        Preconditions.checkNotNull(scope);
        Preconditions.checkNotNull(controller);
        Preconditions.checkNotNull(connectionFactory);
        Preconditions.checkNotNull(streamManager);
        this.scope = scope;
        this.controller = controller;
        this.connectionFactory = connectionFactory;
        this.streamManager = streamManager;
    }

    @Override
    public <T> Producer<T> createProducer(String streamName, Serializer<T> s, ProducerConfig config) {
        Stream stream = streamManager.getStream(streamName);
        EventRouter router = new EventRouter(stream, controller);
        return new ProducerImpl<T>(stream,
                controller,
                new SegmentOutputStreamFactoryImpl(controller, connectionFactory),
                router,
                s,
                config);
    }
    
    @Override
    public <T> IdempotentProducer<T> createIdempotentProducer(String streamName, Serializer<T> s,
            ProducerConfig config) {
        throw new NotImplementedException();
    }

    @Override
    public <T> Consumer<T> createConsumer(String stream, Serializer<T> s, ConsumerConfig config,
            Position startingPosition) {
        return new ConsumerImpl<T>(new SegmentInputStreamFactoryImpl(controller, connectionFactory),
                s,
                startingPosition.asImpl(),
                new SingleStreamOrderer<T>(),
                config);
    }

    @Override
    public <T> Consumer<T> createConsumer(String consumerId, String consumerGroup, Serializer<T> s,
            ConsumerConfig config) {
        throw new NotImplementedException();
    }

    @Override
    public <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>> Synchronizer<StateT, UpdateT, InitT> createSynchronizer(
            String streamName, Serializer<UpdateT> updateSerializer, Serializer<InitT> initialSerializer,
            SynchronizerConfig config) {
        Stream stream = streamManager.getStream(streamName);
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
        return new SynchronizerImpl<StateT, UpdateT, InitT>(stream, in, out, updateSerializer, initialSerializer);
    }

    private static final class SingleStreamOrderer<T> implements Orderer<T> {
        @Override
        public SegmentConsumer<T> nextConsumer(Collection<SegmentConsumer<T>> logs) {
            Preconditions.checkState(logs.size() == 1);
            return logs.iterator().next();
        }
    }


}
