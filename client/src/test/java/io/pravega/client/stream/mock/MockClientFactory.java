/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.mock;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
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
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.Controller;
import java.util.function.Supplier;

public class MockClientFactory implements EventStreamClientFactory, SynchronizerClientFactory, AutoCloseable {

    private final ConnectionFactoryImpl connectionFactory;
    private final Controller controller;
    private final ClientFactoryImpl impl;

    public MockClientFactory(String scope, MockSegmentStreamFactory ioFactory) {
        this.connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        this.controller = new MockController("localhost", 0, connectionFactory, false);
        this.impl = new ClientFactoryImpl(scope, controller, connectionFactory, ioFactory, ioFactory, ioFactory, ioFactory);
    }

    public MockClientFactory(String scope, Controller controller) {
        this.connectionFactory = new ConnectionFactoryImpl(ClientConfig.builder().build());
        this.controller = controller;
        this.impl = new ClientFactoryImpl(scope, controller, connectionFactory);
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
