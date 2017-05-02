/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.stream.mock;

import io.pravega.ClientFactory;
import io.pravega.state.InitialUpdate;
import io.pravega.state.Revisioned;
import io.pravega.state.RevisionedStreamClient;
import io.pravega.state.StateSynchronizer;
import io.pravega.state.SynchronizerConfig;
import io.pravega.state.Update;
import io.pravega.stream.EventStreamReader;
import io.pravega.stream.EventStreamWriter;
import io.pravega.stream.EventWriterConfig;
import io.pravega.stream.IdempotentEventStreamWriter;
import io.pravega.stream.Position;
import io.pravega.stream.ReaderConfig;
import io.pravega.stream.Serializer;
import io.pravega.stream.impl.ClientFactoryImpl;
import io.pravega.stream.impl.Controller;
import io.pravega.stream.impl.netty.ConnectionFactoryImpl;

import java.util.function.Supplier;

public class MockClientFactory implements ClientFactory, AutoCloseable {

    private final ConnectionFactoryImpl connectionFactory;
    private final Controller controller;
    private final ClientFactoryImpl impl;

    public MockClientFactory(String scope, MockSegmentStreamFactory ioFactory) {
        this.connectionFactory = new ConnectionFactoryImpl(false);
        this.controller = new MockController("localhost", 0, connectionFactory);
        this.impl = new ClientFactoryImpl(scope, controller, connectionFactory, ioFactory, ioFactory);
    }

    public MockClientFactory(String scope, Controller controller) {
        this.connectionFactory = new ConnectionFactoryImpl(false);
        this.controller = controller;
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

    public <T> EventStreamReader<T> createReader(String readerId, String readerGroup, Serializer<T> s,
                                                 ReaderConfig config, Supplier<Long> nanoTime,
                                                 Supplier<Long> milliTime) {
        return impl.createReader(readerId, readerGroup, s, config, nanoTime, milliTime);
    }
    
    @Override
    public <T> RevisionedStreamClient<T> createRevisionedStreamClient(String streamName, Serializer<T> serializer,
            SynchronizerConfig config) {
        return impl.createRevisionedStreamClient(streamName, serializer, config);
    }

    @Override
    public <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>> StateSynchronizer<StateT> createStateSynchronizer(
            String streamName, Serializer<UpdateT> updateSerializer, Serializer<InitT> initialSerializer,
            SynchronizerConfig config) {
        return impl.createStateSynchronizer(streamName, updateSerializer, initialSerializer, config);
    }

    @Override
    public void close() {
        this.connectionFactory.close();
    }
}
