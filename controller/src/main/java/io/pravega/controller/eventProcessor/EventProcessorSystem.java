/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.eventProcessor;

import io.pravega.controller.store.checkpoint.CheckpointStore;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.shared.controller.event.ControllerEvent;

import javax.annotation.Nullable;
import java.util.concurrent.ScheduledExecutorService;

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
     * @param eventProcessorConfig Properties of the EventProcessor to be instantiated
     *              in the EventProcessorGroup.
     * @param checkpointStore Checkpoint store.
     * @param <T> Stream Event type parameter.
     * @param rebalanceExecutor executor to run periodic checks for readergroup rebalance. 
     *                          Pass null to disable rebalancing.
     *                          
     * @return EventProcessorGroup reference.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    <T extends ControllerEvent> EventProcessorGroup<T> createEventProcessorGroup(
            final EventProcessorConfig<T> eventProcessorConfig, final CheckpointStore checkpointStore,
            @Nullable final ScheduledExecutorService rebalanceExecutor)
            throws CheckpointStoreException;
}
