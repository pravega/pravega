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

import java.util.List;

/**
 * It acts as the manager and wrapper around EventProcessor groups
 * processing events from Pravega Streams belonging to a specific scope.
 * It provides the only mechanism to create EventProcessor groups.
 */
public interface EventProcessorSystem {

    /**
     * Returns the name of the EventProcessorSystem.
     * @return name.
     */
    String getName();

    /**
     * Returns Pravega Scope.
     * @return scope.
     */
    String getScope();

    /**
     * Returns the process in which the event processor system runs. Process identifier is
     * is a combination of host id and process id within the host.
     * @return process identifier.
     */
    String getProcess();

    /**
     * Creates an EventProcessorGroup and returns a reference to it.
     * @param props Properties of the EventProcessor to be instantiated
     *              in the EventProcessorGroup.
     * @param <T> Stream Event type parameter.
     * @return EventProcessorGroup reference.
     */
    <T extends StreamEvent> EventProcessorGroup<T> createEventProcessorGroup(Props<T> props);

    /**
     * Returns the list of all event processor groups created in the system.
     * @return List of all event processor groups created in the system.
     */
    List<EventProcessorGroup> getEventProcessorGroups();
}
