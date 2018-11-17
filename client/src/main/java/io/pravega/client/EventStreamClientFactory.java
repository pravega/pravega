/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client;

import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.Position;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroup;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TransactionalEventStreamWriter;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import lombok.val;

/**
 * Used to create Writers, Readers, and Synchronizers operating on a stream.
 * <p>
 * Events that are written to a stream can be read by a reader. All events can be processed with
 * exactly once semantics provided the reader has the ability to restore to the correct position
 * upon failure. See {@link EventRead#getPosition()}
 * <p>
 * A note on ordering: Events inside of a stream have a strict order, but may need to be divided
 * between multiple readers for scaling. In order to process events in parallel on different hosts
 * and still have some ordering guarantees; events written to a stream have a routingKey see
 * {@link EventStreamWriter#writeEvent(String, Object)}. Events within a routing key are strictly
 * ordered (i.e. They must go the the same reader or its replacement). However because
 * {@link ReaderGroup}s process events in parallel there is no ordering between different readers.
 *
 * <p>
 * A note on scaling: Because a stream can grow in its event rate, streams are divided into
 * Segments. For the most part this is an implementation detail. However its worth understanding
 * that the way a stream is divided between multiple readers in a group that wish to split the
 * messages between them is by giving different segments to different readers.
 */
public interface EventStreamClientFactory extends AutoCloseable {

    /**
     * Creates a new instance of Client Factory.
     *
     * @param scope The scope string.
     * @param config Configuration for the client.
     * @return Instance of ClientFactory implementation.
     */
    static EventStreamClientFactory withScope(String scope, ClientConfig config) {
        val connectionFactory = new ConnectionFactoryImpl(config);
        return new ClientFactoryImpl(scope, new ControllerImpl(ControllerImplConfig.builder().clientConfig(config).build(),
                connectionFactory.getInternalExecutor()), connectionFactory);
    }

    /**
     * Creates a new writer that can write to the specified stream.
     *
     * @param streamName The name of the stream to write to.
     * @param config The writer configuration.
     * @param s The Serializer.
     * @param <T> The type of events.
     * @return Newly created writer object
     */
    <T> EventStreamWriter<T> createEventWriter(String streamName, Serializer<T> s, EventWriterConfig config);
    
    /**
     * Creates a new writer that can write to the specified stream.
     *
     * @param streamName The name of the stream to write to.
     * @param config The writer configuration.
     * @param s The Serializer.
     * @param <T> The type of events.
     * @return Newly created writer object
     */
    <T> TransactionalEventStreamWriter<T> createTransactionalEventWriter(String streamName, Serializer<T> s, EventWriterConfig config);

    /**
     * Creates (or recreates) a new reader that is part of a {@link ReaderGroup}. The reader
     * will join the group and the members of the group will automatically rebalance among
     * themselves.
     * <p>
     * In the event that the reader dies, the method {@link ReaderGroup#readerOffline(String, Position)}
     * should be called, passing the last position of the reader. (Usually done by storing the
     * position along with the output when it is processed.) Which will trigger redistribute the
     * events among the remaining readers.
     * <p>
     * Note that calling reader offline while the reader is still online may result in multiple
     * reader within the group receiving the same events.
     *
     * @param readerId A unique name (within the group) for this readers.
     * @param readerGroup The name of the group to join.
     * @param s The serializer for events.
     * @param config The readers configuration.
     * @param <T> The type of events.
     * @return Newly created reader object that is a part of reader group
     */
    <T> EventStreamReader<T> createReader(String readerId, String readerGroup, Serializer<T> s, ReaderConfig config);

    /**
     * Closes the client factory. This will close any connections created through it.
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();

}
