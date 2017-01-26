/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.mock;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.RevisionedStreamClient;
import com.emc.pravega.state.StateSynchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.IdempotentEventStreamWriter;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;

public class MockClientFactory implements ClientFactory, AutoCloseable {

    private final String scope;
    private final ClientFactory impl;
    private final ConnectionFactoryImpl connectionFactory;

    public MockClientFactory(String scope, String endpoint, int port) {
        this.scope = scope;
        this.connectionFactory = new ConnectionFactoryImpl(false);
        MockController controller = new MockController(endpoint, port, connectionFactory);
        this.impl = new ClientFactoryImpl(scope, controller, connectionFactory);
    }

    public MockClientFactory(String scope, MockSegmentStreamFactory ioFactory) {
        this.scope = scope;
        this.connectionFactory = new ConnectionFactoryImpl(false);
        MockController controller = new MockController("localhost", 0, connectionFactory);
        this.impl = new ClientFactoryImpl(scope, controller, ioFactory, ioFactory);
    }
    
    public MockClientFactory(String scope, Controller controller) {
        this.scope = scope;
        this.connectionFactory = new ConnectionFactoryImpl(false);
        this.impl = new ClientFactoryImpl(scope, controller, connectionFactory);
    }
   
    @Override
    public <T> EventStreamWriter<T> createEventWriter(String streamName, Serializer<T> s, EventWriterConfig config) {
        return impl.createEventWriter(streamName, s, config);
    }

    @Override
    public <T> IdempotentEventStreamWriter<T> createIdempotentEventWriter(String streamName, Serializer<T> s,
            EventWriterConfig config) {
        return impl.createIdempotentEventWriter(streamName, s, config);
    }

    @Override
    public <T> EventStreamReader<T> createReader(String streamName, Serializer<T> s, ReaderConfig config,
            Position startingPosition) {
        return impl.createReader(streamName, s, config, startingPosition);
    }

    @Override
    public <T> EventStreamReader<T> createReader(String readerId, String readerGroup, Serializer<T> s,
            ReaderConfig config) {
        return impl.createReader(readerId, readerGroup, s, config);
    }

    @Override
    public <T> RevisionedStreamClient<T> createRevisionedStreamClient(String streamName, Serializer<T> serializer,
            SynchronizerConfig config) {
        return impl.createRevisionedStreamClient(streamName, serializer, config);
    }
    
    @Override
    public <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>> 
    StateSynchronizer<StateT> createStateSynchronizer(String streamName, 
            Serializer<UpdateT> updateSerializer, 
            Serializer<InitT> initialSerializer,
            SynchronizerConfig config) {
        return impl.createStateSynchronizer(streamName, updateSerializer, initialSerializer, config);
    }

    @Override
    public void close() {
        this.connectionFactory.close();
    }
}
