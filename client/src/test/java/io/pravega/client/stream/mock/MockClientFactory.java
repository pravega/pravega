/**
 * Copyright Pravega Authors.
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
package io.pravega.client.stream.mock;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.impl.AbstractClientFactoryImpl;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.control.impl.Controller;
import java.util.function.Supplier;

public class MockClientFactory extends AbstractClientFactoryImpl implements EventStreamClientFactory, SynchronizerClientFactory, AutoCloseable {

    private final ClientFactoryImpl impl;

    private MockClientFactory(String scope, ClientConfig config, ConnectionPoolImpl connectionPool, MockSegmentStreamFactory ioFactory) {
        super(scope, new MockController("localhost", 0, connectionPool, false), connectionPool);
        this.impl = new ClientFactoryImpl(scope, controller, connectionPool, ioFactory, ioFactory, ioFactory, ioFactory);
    }
    
    private MockClientFactory(String scope, ClientConfig config, MockSegmentStreamFactory ioFactory) {
        this(scope, config, new ConnectionPoolImpl(config, new SocketConnectionFactoryImpl(config)), ioFactory);
    }
    
    public MockClientFactory(String scope, MockSegmentStreamFactory ioFactory) {
        this(scope, ClientConfig.builder().build(), ioFactory);
    }

    public MockClientFactory(String scope, Controller controller, ConnectionPool connectionPool) {
        super(scope, controller, connectionPool);
        this.impl = new ClientFactoryImpl(scope, controller, connectionPool);
    }

    @Override
    public <T> EventStreamWriter<T> createEventWriter(String streamName, Serializer<T> s, EventWriterConfig config) {
        return impl.createEventWriter(streamName, s, config);
    }
    
    @Override
    public <T> EventStreamWriter<T> createEventWriter(String writerId, String streamName, Serializer<T> s, EventWriterConfig config) {
        return impl.createEventWriter(writerId, streamName, s, config);
    }
    
    @Override
    public <T> TransactionalEventStreamWriter<T> createTransactionalEventWriter(String writerId, String streamName, Serializer<T> s, EventWriterConfig config) {
        return impl.createTransactionalEventWriter(writerId, streamName, s, config);
    }

    @Override
    public <T> TransactionalEventStreamWriter<T> createTransactionalEventWriter(String streamName, Serializer<T> s, EventWriterConfig config) {
        return impl.createTransactionalEventWriter(streamName, s, config);
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
        this.controller.close();
        this.impl.close();
    }
}
