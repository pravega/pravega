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
import com.emc.pravega.state.RevisionedStreamClient;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.state.Update;
import com.emc.pravega.state.impl.CorruptedStateException;
import com.emc.pravega.state.impl.RevisionedStreamClientImpl;
import com.emc.pravega.state.impl.StateSynchronizerImpl;
import com.emc.pravega.state.impl.UpdateOrInitSerializer;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.IdempotentEventStreamWriter;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderConfig;
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
    public <T> EventStreamWriter<T> createEventWriter(String streamName, Serializer<T> s, EventWriterConfig config) {
        Stream stream = streamManager.getStream(streamName);
        EventRouter router = new EventRouter(stream, controller);
        return new EventStreamWriterImpl<T>(stream,
                controller,
                new SegmentOutputStreamFactoryImpl(controller, connectionFactory),
                router,
                s,
                config);
    }
    
    @Override
    public <T> IdempotentEventStreamWriter<T> createIdempotentEventWriter(String streamName, Serializer<T> s,
            EventWriterConfig config) {
        throw new NotImplementedException();
    }

    @Override
    public <T> EventStreamReader<T> createReader(String stream, Serializer<T> s, ReaderConfig config,
            Position startingPosition) {
        return new EventReaderImpl<T>(new SegmentInputStreamFactoryImpl(controller, connectionFactory),
                s,
                startingPosition.asImpl(),
                new SingleStreamOrderer<T>(),
                config);
    }

    @Override
    public <T> EventStreamReader<T> createReader(String readerId, String readerGroup, Serializer<T> s,
            ReaderConfig config) {
        throw new NotImplementedException();
    }

    @Override
    public <T> RevisionedStreamClient<T> createRevisionedStreamClient(String streamName, Serializer<T> serializer,
            SynchronizerConfig config) {
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
        return new RevisionedStreamClientImpl<>(segment, in, out, serializer);
    }

    @Override
    public <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>> 
            StateSynchronizer<StateT, UpdateT, InitT> createStateSynchronizer(String streamName,
                    Serializer<UpdateT> updateSerializer, Serializer<InitT> initialSerializer,
                    SynchronizerConfig config) {
        Segment segment = new Segment(scope, streamName, 0);
        UpdateOrInitSerializer<StateT, UpdateT, InitT> serializer = new UpdateOrInitSerializer<>(segment,
                updateSerializer,
                initialSerializer);
        return new StateSynchronizerImpl<StateT, UpdateT, InitT>(segment,
                createRevisionedStreamClient(streamName, serializer, config));
    }

    private static final class SingleStreamOrderer<T> implements Orderer<T> {
        @Override
        public SegmentReader<T> nextSegment(Collection<SegmentReader<T>> logs) {
            Preconditions.checkState(logs.size() == 1);
            return logs.iterator().next();
        }
    }

}
