/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream;

import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Synchronizer;
import com.emc.pravega.state.SynchronizerConfig;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.impl.Orderer;

/**
 * A stream can be thought of as an infinite sequence of events.
 * A stream can be published to or consumed from.
 * A stream is:
 * Append only (Events in it are immutable once published)
 * Infinite (There are no limitations in size or time to how many events can go into a stream)
 * Strongly Consistent (Events are either in the stream or they are not, and not subject to reordering once written)
 * Scalable (The rate of events in a stream can greatly exceed the capacity of any single host)
 * <p>
 * Events that a published to a stream can be consumed by a consumer. All events can be consumed with exactly once
 * semantics provided the consumer has the ability to restore to the correct position upon failure. See
 * {@link Consumer#getPosition}
 * <p>
 * A note on ordering:
 * Events inside of a stream have a strict order, but may need to be devised between multiple consumers for scaling.
 * Because events being processed in parallel on different hosts cannot have ordering semantics a few things are done.
 * Events published to a stream have a routingKey see {@link Producer#publish}.
 * Events within a routing key are strictly ordered (IE: They must go the the same consumer or its replacement).
 * For other events, within a single consumer, the ordering is dictated by the {@link Orderer}
 * If the Orderer used by the consumer is consistent, order of all events seen by that consumer is strict.
 * IE: the stream can be multiple times from the same position and the events will always be in the same order.
 * Other implementations of Orderer are used to try to lower latency etc. But none of them will change the semantic that
 * within a routingKey ordering is strict.
 * <p>
 * A note on scaling:
 * Because a stream can grow in its event rate, streams are divided into Segments. For the most part this is an
 * implementation detail. However its worth understanding that the way a stream is divided between multiple consumers in
 * a group that wish to split the messages between them is by giving different segments to different consumers. For this
 * reason when creating a consumer a {@link RateChangeListener} is provided, that can help scale up or down the number
 * of consumers if the number of segments has changed in response to a change in the rate of events in the stream. For
 * the most part this is done by calling {@link RebalancerUtils#rebalance}.
 */
public interface Stream {
    /**
     * Gets the scope of this stream.
     */
    String getScope();

    /**
     * Gets the name of this stream  (Not including the scope).
     */
    String getStreamName();

    /**
     * Gets the scoped name of this stream.
     */
    String getScopedName();

    /**
     * Gets the configuration associated with this stream.
     */
    StreamConfiguration getConfig();

    /**
     * Creates a new producer that can publish to this stream.
     *
     * @param config The producer configuration.
     * @param s      The Serializer.
     * @param <T>    The type of events.
     */
    <T> Producer<T> createProducer(Serializer<T> s, ProducerConfig config);

    /**
     * Creates a new consumer that will consumer from this stream at the startingPosition.
     * To obtain an initial position use {@link RebalancerUtils#getInitialPositions}
     * Consumers are responsible for their own failure management. In the event that a consumer dies the system will do
     * nothing about it until you do so manually. (Usually by getting its last {@link Position}) object and either
     * calling this method again or invoking: {@link RebalancerUtils#rebalance} and then invoking this method.
     *
     * @param s                The Serializer.
     * @param config           The consumer configuration.
     * @param l                The RateChangeListener to use.
     * @param startingPosition The StartingPosition to use.
     * @param <T>              The type of events.
     */
    <T> Consumer<T> createConsumer(Serializer<T> s, ConsumerConfig config, Position startingPosition, RateChangeListener l);
    
    /**
     * Creates (or recreates) a new consumer on this stream that is part of a {@link ConsumerGroup}.
     * The consumer will join the group and the members of the group will automatically rebalance
     * amoung themselves. In the event that the consumer dies, the method
     * {@link ConsumerGroup#consumerOffline(String, Position)} should be called, passing the last
     * position of the consumer. This will automatically redistribute the events among the remaining
     * consumers. Note that calling consumer offline while the consumer is still online may result in
     * multiple consumers within the group receiving the same events.
     * 
     * @param consumerId        A unique name (within the group) for this consumer.
     * @param consumerGroup     The name of the group to join.
     * @param s                 The serializer for events.
     * @param config            The consumer configuration. 
     * @param <T>               The type of events.
     */
    <T> Consumer<T> createConsumer(String consumerId, String consumerGroup, Serializer<T> s, ConsumerConfig config);

    /**
     * Creates a new Synchronizer that will work on this stream.
     * 
     * @param <StateT> The type of the state being synchronized.
     * @param <UpdateT> The type of updates applied to the state object.
     * @param <InitT> The type of the initializer of the stat object.
     * @param updateSerializer The serializer for updates.
     * @param initialSerializer The serializer for the initial update.
     * @param config The Serializer configuration
     */
    <StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>> 
            Synchronizer<StateT, UpdateT, InitT> createSynchronizer(Serializer<UpdateT> updateSerializer,
                    Serializer<InitT> initialSerializer, SynchronizerConfig config);

}
