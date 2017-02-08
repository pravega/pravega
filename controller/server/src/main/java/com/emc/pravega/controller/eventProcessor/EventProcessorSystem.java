/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.eventProcessor;

import com.emc.pravega.stream.EventStreamWriter;

/**
 * It acts as the manager and wrapper around Actors and ActorGroups
 * processing events from Pravega Streams belonging to a specific scope
 * and hosted at specific location. It provides the only mechanism
 * to create ActorGroups.
 */
public interface EventProcessorSystem {

    /**
     * Returns the name of the ActorSystem.
     * @return name.
     */
    String getName();

    /**
     * Returns Pravega Scope.
     * @return scope.
     */
    String getScope();

    /**
     * Creates an ActorGroup and returns a reference to it.
     * @param props Properties of the AbstractActor to be instantiated
     *              in the ActorGroup.
     * @param <T> Stream Event type parameter.
     * @return ActorGroup reference.
     */
    <T extends StreamEvent> EventStreamWriter<T> createEventProcessorGroup(Props<T> props);

    /**
     * Notify a host failure to ActorGroups managed by the ActorSystem.
     * The ActorGroup may send a notification to the Pravega ReaderGroup
     * so that the orphaned Stream Segments can be redistributed among
     * existing hosts.
     * @param host Failed host's identifier.
     */
    void notifyHostFailure(String host);

    /**
     * Stops all EventProcessors belonging to EventProcessorGroup created
     * under the EventProcessorSystem.
     */
    void stop();
}
