/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
