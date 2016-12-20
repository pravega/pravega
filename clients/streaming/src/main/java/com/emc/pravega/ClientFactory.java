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
package com.emc.pravega;

import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Synchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.EventStreamReader;
import com.emc.pravega.stream.ReaderConfig;
import com.emc.pravega.stream.ReaderGroup;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.IdempotentEventStreamWriter;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.RebalancerUtils;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.ClientFactoryImpl;

import java.net.URI;

/**
 * Used to create Writers, Readers, and Synchronizers operating on a stream.
 * 
 * Events that a written to a stream can be read by a reader. All events can be processed with
 * exactly once semantics provided the reader has the ability to restore to the correct position
 * upon failure. See {@link EventStreamReader#getPosition}
 * <p>
 * A note on ordering: Events inside of a stream have a strict order, but may need to be divided
 * between multiple readers for scaling. In order to process events in parallel on different hosts
 * and still have some ordering guarentees; events written to a stream have a routingKey see
 * {@link EventStreamWriter#writeEvent(String, Object)}. Events within a routing key are strictly
 * ordered (IE: They must go the the same reader or its replacement). For other events, within a
 * single reader, the ordering is dictated by {@link EventRead#getWriteTimeCounter()} However as
 * {@link ReaderGroup}s process events in parallel there is no ordering between different readers.
 * 
 * <p>
 * A note on scaling: Because a stream can grow in its event rate, streams are divided into
 * Segments. For the most part this is an implementation detail. However its worth understanding
 * that the way a stream is divided between multiple readers in a group that wish to split the
 * messages between them is by giving different segments to different readers. For this reason when
 * creating a reader a notification is provided. {@link EventRead#isRoutingRebalance()} In the case
 * of a reader group, this is automated.
 * 
 * Otherwise this can be done by creating new reader by calling: {@link RebalancerUtils#rebalance} .
 */
public interface ClientFactory {

    public static ClientFactory withScope(String scope, URI controllerUri) {
        return new ClientFactoryImpl(scope, controllerUri);
    }

    /**
     * Creates a new writer that can write to the specified stream.
     *
     * @param streamName The name of the stream to write to.
     * @param config The writer configuration.
     * @param s The Serializer.
     * @param <T> The type of events.
     */
    <T> EventStreamWriter<T> createEventWriter(String streamName, Serializer<T> s, EventWriterConfig config);
    
    /**
     * Creates a new writer that can write to the specified stream with a strictly increasing
     * sequence associated with each one.
     *
     * @param streamName The name of the stream to write to.
     * @param config The writer configuration.
     * @param s The Serializer.
     * @param <T> The type of events.
     */
    <T> IdempotentEventStreamWriter<T> createIdempotentEventWriter(String streamName, Serializer<T> s, EventWriterConfig config);

    /**
     * Creates a new manually managed reader that will read from the specified stream at the
     * startingPosition. To obtain an initial position use
     * {@link RebalancerUtils#getInitialPositions} Readers are responsible for their own failure
     * management and rebalancing. In the event that a reader dies the system will do nothing
     * about it until you do so manually. (Usually by getting its last {@link Position} and either
     * calling this method again or invoking: {@link RebalancerUtils#rebalance} and then invoking
     * this method.
     * 
     * @param streamName The name of the stream for the reader
     * @param s The Serializer.
     * @param config The reader configuration.
     * @param startingPosition The StartingPosition to use.
     * @param <T> The type of events.
     */
    <T> EventStreamReader<T> createReader(String streamName, Serializer<T> s, ReaderConfig config,
            Position startingPosition);

    /**
     * Creates (or recreates) a new reader that is part of a {@link ReaderGroup}. The reader
     * will join the group and the members of the group will automatically rebalance among
     * themselves.
     * 
     * In the event that the reader dies, the method {@link ReaderGroup#readerOffline()}
     * should be called, passing the last position of the reader. (Usually done by storing the
     * position along with the output when it is processed.) Which will trigger redistribute the
     * events among the remaining readers.
     * 
     * Note that calling reader offline while the reader is still online may result in multiple
     * reader within the group receiving the same events.
     * 
     * @param readerId A unique name (within the group) for this readers.
     * @param readerGroup The name of the group to join.
     * @param s The serializer for events.
     * @param config The readers configuration.
     * @param <T> The type of events.
     */
    <T> EventStreamReader<T> createReader(String readerId, String readerGroup, Serializer<T> s, ReaderConfig config);

    /**
     * Creates a new Synchronizer that will work on the specified stream.
     * 
     * @param <StateT> The type of the state being synchronized.
     * @param <UpdateT> The type of updates applied to the state object.
     * @param <InitT> The type of the initializer of the stat object.
     * @param streamName The name of the stream for the synchronizer
     * @param updateSerializer The serializer for updates.
     * @param initialSerializer The serializer for the initial update.
     * @param config The Serializer configuration
     */
    <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>> 
            Synchronizer<StateT, UpdateT, InitT> createSynchronizer(String streamName,
                                                                    Serializer<UpdateT> updateSerializer,
                                                                    Serializer<InitT> initialSerializer,
                                                                    SynchronizerConfig config);

}
