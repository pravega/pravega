/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor;

import com.emc.pravega.controller.store.checkpoint.CheckpointStore;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;

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
     * @return EventProcessorGroup reference.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    <T extends ControllerEvent> EventProcessorGroup<T> createEventProcessorGroup(
            final EventProcessorConfig<T> eventProcessorConfig, final CheckpointStore checkpointStore)
            throws CheckpointStoreException;
}
