/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor;

import com.emc.pravega.stream.EventStreamWriter;

import java.util.Set;

/**
 * EventProcessor group interface. It provides mechanism to manage event
 * processors processing events from a Pravega Stream by participating in
 * the same ReaderGroup.
 */
public interface EventProcessorGroup<T extends ControllerEvent> {

    /**
     * Notifies Pravega ReaderGroup about failure of a process
     * participating in the Reader Group.
     * @param process Failed process's identifier.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    void notifyProcessFailure(String process) throws CheckpointStoreException;

    /**
     * Increase/decrease the number of event processors reading from the Pravega
     * Stream and participating in the ReaderGroup. This method may be
     * invoked if the number of active segments in the Pravega Stream
     * increases/decreased on account of a Scale event due to increased/
     * decreased event throughput.
     * @param count Number of event processors to add. Negative number indicates
     *              decreasing the Actor count.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    void changeEventProcessorCount(int count) throws CheckpointStoreException;

    /**
     * Returns a reference to its writer.
     * @return writer reference of this event processor group.
     */
    EventStreamWriter<T> getWriter();

    /**
     * Gets the list of processes participating in the logical EventProcessorGroup.
     * @return list of processes participating in the Logical EventProcessorGroup.
     */
    Set<String> getProcesses();

    /**
     * Initiates stop on all event processors in this group and waits for
     * their termination.
     * @throws CheckpointStoreException on error accessing or updating checkpoint store.
     */
    void stopAll() throws CheckpointStoreException;
}
