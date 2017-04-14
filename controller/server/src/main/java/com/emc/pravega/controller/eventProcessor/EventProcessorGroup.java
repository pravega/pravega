/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor;

import com.emc.pravega.controller.requests.ControllerEvent;
import com.emc.pravega.controller.store.checkpoint.CheckpointStoreException;
import com.emc.pravega.stream.EventStreamWriter;
import com.google.common.util.concurrent.Service;

import java.util.Set;

/**
 * EventProcessor group interface. It provides mechanism to manage event
 * processors processing events from a Pravega Stream by participating in
 * the same ReaderGroup.
 */
public interface EventProcessorGroup<T extends ControllerEvent> extends Service, AutoCloseable {

    /**
     * Notifies Pravega ReaderGroup about failure of a process
     * participating in the Reader Group.
     * @param process Failed process's identifier.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    void notifyProcessFailure(String process) throws CheckpointStoreException;

    /**
     * Returns a reference to its writer.
     * @return writer reference of this event processor group.
     */
    EventStreamWriter<T> getWriter();

    /**
     * Gets the list of processes participating in the logical EventProcessorGroup.
     * @return list of processes participating in the Logical EventProcessorGroup.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    Set<String> getProcesses() throws CheckpointStoreException;
}
