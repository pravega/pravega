/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.stream.mock;

import io.pravega.client.ClientFactory;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.IdempotentEventStreamWriter;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.netty.ConnectionFactoryImpl;

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
