/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.eventProcessor;

import io.pravega.client.segment.impl.Segment;
import io.pravega.controller.store.checkpoint.CheckpointStoreException;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.client.stream.EventStreamWriter;
import com.google.common.util.concurrent.Service;

import java.util.Map;
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

    /**
     * Gets the checkpoints for all event processor cells in the event processor group into a consolidated map. 
     * During a rebalance event, if previous reader (event processor cell) has relinquished a segment and 
     * new reader (event processor cell) has attained it, it is possible that very briefly, 
     * the old cell is yet to be removed from the event processor group. 
     * If getSegments is requested during this small window, the same segment may be found in both their checkpoints 
     * (very unlikely because new reader would not have had a need to checkpoint within that brief period). 
     * However, the consolidation step will take the larger offset of the two for same segment found across different cells.  
     * 
     * @return A map of segment to offset for all no... 
     */
    Map<Segment, Long> getCheckpoint();
}
