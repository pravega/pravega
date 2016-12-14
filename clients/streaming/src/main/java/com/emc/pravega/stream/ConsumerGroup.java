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
package com.emc.pravega.stream;

import java.util.Set;

/**
 * A consumer group is a collection of consumers that collectively read all the events in the
 * stream. The events are distributed among the consumers in the group such that each event goes
 * to only one consumer.
 * 
 * The consumers in the group may change over time. Consumers are added to the group by calling
 * {@link Stream#createConsumer(String, String, Serializer, ConsumerConfig)} and are removed by
 * calling {@link #consumerOffline(String, Position)}
 */
public interface ConsumerGroup {

    /**
     * Returns the scope of the stream which the group is associated with.
     */
    String getScope();

    /**
     * Returns the name of the stream the group is associated with.
     */
    String getStreamName();

    /**
     * Returns the name of the group.
     */
    String getGroupName();

    /**
     * Returns the configuration of the consumer group.
     */
    ConsumerGroupConfig getConfig();

    /**
     * Invoked when a consumer that was added to the group is no longer consuming events. This will
     * cause the events that were going to that consumer to be redistributed among the other
     * consumers. Events after the lastPosition provided will be (re)read by other consumers in the
     * {@link ConsumerGroup}.
     * 
     * Note that this method is automatically invoked by {@link Consumer#close()}
     * 
     * @param consumerId The id of the consumer that is offline.
     * @param lastPosition The position of the last event that was successfully processed by the
     *        consumer.
     */
    void consumerOffline(String consumerId, Position lastPosition);
    
    /**
     * Returns a set of consumerIds for the consumers that are considered to be online by the group.
     * IE: {@link Stream#createConsumer(String, String, Serializer, ConsumerConfig)} was called but
     * {@link #consumerOffline(String, Position)} was not called subsequently.
     */
    Set<String> getOnlineConsumers();
}
