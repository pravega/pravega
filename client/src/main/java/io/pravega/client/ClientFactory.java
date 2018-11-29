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

import com.google.common.annotations.Beta;
import io.pravega.client.netty.impl.ConnectionFactoryImpl;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.Revisioned;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
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
import java.net.URI;
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
public interface ClientFactory extends AutoCloseable {

    /**
     * Creates a new instance of Client Factory.
     *
     * @param scope The scope string.
     * @param controllerUri The URI for controller.
     * @return Instance of ClientFactory implementation.
     * @deprecated Use {@link #withScope(String, ClientConfig)}
     */
    @Deprecated
    static ClientFactory withScope(String scope, URI controllerUri) {
        return withScope(scope, ClientConfig.builder().controllerURI(controllerUri).build());
    }

    /**
     * Creates a new instance of Client Factory.
     *
     * @param scope The scope string.
     * @param config Configuration for the client.
     * @return Instance of ClientFactory implementation.
     */
    static ClientFactory withScope(String scope, ClientConfig config) {
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
     * @deprecated Use {@link EventStreamClientFactory#createEventWriter(String, Serializer, EventWriterConfig)} instead
     */
    @Deprecated
    <T> EventStreamWriter<T> createEventWriter(String streamName, Serializer<T> s, EventWriterConfig config);
    
    /**
     * Creates a new transactional writer that can write events atomically to the specified stream.
     *
     * @param streamName The name of the stream to write to.
     * @param config The writer configuration.
     * @param s The Serializer.
     * @param <T> The type of events.
     * @return Newly created writer object
     * @deprecated Use {@link EventStreamClientFactory#createTransactionalEventWriter(String, Serializer, EventWriterConfig)} instead.
     */
    @Deprecated
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
     * @deprecated Use {@link EventStreamClientFactory#createReader(String, String, Serializer, ReaderConfig)} instead
     */
    @Deprecated
    <T> EventStreamReader<T> createReader(String readerId, String readerGroup, Serializer<T> s, ReaderConfig config);

    /**
     * Creates a new RevisionedStreamClient that will work with the specified stream.
     *
     * @param streamName The name of the stream for the synchronizer
     * @param serializer The serializer for updates.
     * @param config The client configuration
     * @param <T> The type of events
     * @return Revisioned stream client
     * @deprecated Use {@link SynchronizerClientFactory#createRevisionedStreamClient(String, Serializer, SynchronizerConfig)} instead.
     */
    @Deprecated
    <T> RevisionedStreamClient<T> createRevisionedStreamClient(String streamName, Serializer<T> serializer,
            SynchronizerConfig config);
    
    /**
     * Creates a new StateSynchronizer that will work on the specified stream.
     *
     * @param <StateT> The type of the state being synchronized.
     * @param <UpdateT> The type of the updates being written.
     * @param <InitT> The type of the initial update used.
     * @param streamName The name of the stream for the synchronizer
     * @param updateSerializer The serializer for updates.
     * @param initSerializer The serializer for the initial update.
     * @param config The synchronizer configuration
     * @return Newly created StateSynchronizer that will work on the given stream
     * @deprecated Use {@link SynchronizerClientFactory#createStateSynchronizer(String, Serializer, Serializer, SynchronizerConfig)} instead.
     */
    @Deprecated
    <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>>
    StateSynchronizer<StateT> createStateSynchronizer(String streamName,
                                                      Serializer<UpdateT> updateSerializer,
                                                      Serializer<InitT> initSerializer,
                                                      SynchronizerConfig config);

    /**
     * Creates a new ByteStreamClient. The byteStreamClient can create readers and writers that work
     * on a stream of bytes. The stream must be pre-created with a single fixed segment. Sharing a
     * stream between the byte stream API and the Event stream readers/writers will CORRUPT YOUR
     * DATA in an unrecoverable way.
     * 
     * @return A byteStreamClient
     * @deprecated Use {@link ByteStreamClientFactory#withScope(String, ClientConfig)}
     */
    @Beta
    @Deprecated
    io.pravega.client.byteStream.ByteStreamClient createByteStreamClient();
    
    /**
     * Create a new batch client. A batch client can be used to perform bulk unordered reads without
     * the need to create a reader group.
     *
     * Please note this is an experimental API.
     *
     * @return A batch client
     * @deprecated Use {@link BatchClientFactory#withScope(String, ClientConfig)}
     */
    @Beta
    @Deprecated
    io.pravega.client.batch.BatchClient createBatchClient();

    /**
     * Closes the client factory. This will close any connections created through it.
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    void close();

}
