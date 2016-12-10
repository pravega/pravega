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

/**
 * An event read from a stream.
 * 
 * @param <T> The type of the event.
 */
public interface EventRead<T> {
    
    /**
     * @return The time associated with the event. (Specified during publish)
     */
    long getEventTime();

    /**
     * @return The event itself.
     */
    T getValue();

    /**
     * The position in the stream that represents where the consumer is immediately following this
     * event. It is useful to store this so that
     * {@link ConsumerGroup#consumerOffline(String, Position)} can be called if the consumer dies.
     */
    Position getPosition();

    /**
     * @return A pointer to this event. This can be used to read the event again by calling
     *         {@link Consumer#read(EventPointer)}
     */
    EventPointer getEventPointer();

    /**
     * Returns a boolean indicating if a rebalance of which events are being routed to which
     * consumers is about to occur. Meaning that on the next call to
     * {@link Consumer#readNextEvent(long)} this consumer may receive events from a different set of
     * RoutingKeys. Once this consumer calls {@link Consumer#readNextEvent(long)} some of the
     * routing keys it was previously reading may be taken over by another consumer.
     * 
     * This is relevant if the consumers process events in a way that involves holding state in
     * memory. For example an app that is maintaining counters in memory for different types of
     * events should interpret this boolean as an indication that it should flush its values to
     * external storage, as the set routing keys it is working with are about to change.
     * 
     * It is the goal of the implementation to not set this to true unless it is required.
     */
    boolean isRoutingRebalance();
}
