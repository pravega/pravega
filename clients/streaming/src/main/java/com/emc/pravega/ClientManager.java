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
import com.emc.pravega.stream.Consumer;
import com.emc.pravega.stream.ConsumerConfig;
import com.emc.pravega.stream.ConsumerGroup;
import com.emc.pravega.stream.EventRead;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.Producer;
import com.emc.pravega.stream.ProducerConfig;
import com.emc.pravega.stream.RebalancerUtils;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.ClientManagerImpl;

import java.net.URI;

/**
 * Used to create Producers, Consumers, and Synchronizers operating on a stream.
 * 
 * Events that a published to a stream can be consumed by a consumer. All events can be consumed
 * with exactly once semantics provided the consumer has the ability to restore to the correct
 * position upon failure. See {@link Consumer#getPosition}
 * <p>
 * A note on ordering: Events inside of a stream have a strict order, but may need to be divided
 * between multiple consumers for scaling. Because events being processed in parallel on different
 * hosts cannot have ordering semantics a few things are done. Events published to a stream have a
 * routingKey see {@link Producer#publish}. Events within a routing key are strictly ordered (IE:
 * They must go the the same consumer or its replacement). For other events, within a single
 * consumer, the ordering is dictated by {@link EventRead#getWriteTimeCounter()} However as
 * {@link ConsumerGroup}s process events in parallel there is no ordering between different
 * consumers.
 * 
 * <p>
 * A note on scaling: Because a stream can grow in its event rate, streams are divided into
 * Segments. For the most part this is an implementation detail. However its worth understanding
 * that the way a stream is divided between multiple consumers in a group that wish to split the
 * messages between them is by giving different segments to different consumers. For this reason
 * when creating a consumer a notification is provided. {@link EventRead#isRoutingRebalance()} In
 * the case of a consumer group, this is automated.
 * 
 * Otherwise this can be done by creating new consumer by calling: {@link RebalancerUtils#rebalance}
 * .
 */
public interface ClientManager {

    public static ClientManager withScope(String scope, URI controllerUri) {
        return new ClientManagerImpl(scope, controllerUri);
    }

    /**
     * Creates a new producer that can publish to the specified stream.
     *
     * @param streamName The name of the stream to produce to.
     * @param config The producer configuration.
     * @param s The Serializer.
     * @param <T> The type of events.
     */
    <T> Producer<T> createProducer(String streamName, Serializer<T> s, ProducerConfig config);

    /**
     * Creates a new manually managed consumer that will consume from the specified stream at the
     * startingPosition. To obtain an initial position use
     * {@link RebalancerUtils#getInitialPositions} Consumers are responsible for their own failure
     * management and rebalancing. In the event that a consumer dies the system will do nothing
     * about it until you do so manually. (Usually by getting its last {@link Position} and either
     * calling this method again or invoking: {@link RebalancerUtils#rebalance} and then invoking
     * this method.
     * 
     * @param streamName The name of the stream for the consumer
     * @param s The Serializer.
     * @param config The consumer configuration.
     * @param startingPosition The StartingPosition to use.
     * @param <T> The type of events.
     */
    <T> Consumer<T> createConsumer(String streamName, Serializer<T> s, ConsumerConfig config,
            Position startingPosition);

    /**
     * Creates (or recreates) a new consumer that is part of a {@link ConsumerGroup}. The consumer
     * will join the group and the members of the group will automatically rebalance among
     * themselves.
     * 
     * In the event that the consumer dies, the method {@link ConsumerGroup#consumerOffline()}
     * should be called, passing the last position of the consumer. (Usually done by storing the
     * position along with the output when it is processed.) Which will trigger redistribute the
     * events among the remaining consumers.
     * 
     * Note that calling consumer offline while the consumer is still online may result in multiple
     * consumers within the group receiving the same events.
     * 
     * @param consumerId A unique name (within the group) for this consumer.
     * @param consumerGroup The name of the group to join.
     * @param s The serializer for events.
     * @param config The consumer configuration.
     * @param <T> The type of events.
     */
    <T> Consumer<T> createConsumer(String consumerId, String consumerGroup, Serializer<T> s, ConsumerConfig config);

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
    <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>> Synchronizer<StateT, UpdateT, InitT> createSynchronizer(
            String streamName, Serializer<UpdateT> updateSerializer, Serializer<InitT> initialSerializer,
            SynchronizerConfig config);

}
