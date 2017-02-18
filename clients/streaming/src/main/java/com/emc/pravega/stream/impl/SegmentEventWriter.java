/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.impl.segment.SegmentSealedException;

import java.util.List;

/**
 * This is the mirror of EventStreamWriter but that only deals with one segment.
 */
public interface SegmentEventWriter<Type> extends AutoCloseable {

    /**
     * Writes a pending event to the segment.
     *
     * @param m The event to be written.
     * @throws SegmentSealedException if segment is sealed already.
     */
    void write(PendingEvent<Type> m) throws SegmentSealedException;

    /**
     * Blocks on all outstanding writes.
     *
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    void flush() throws SegmentSealedException;

    /**
     * Closes the Event Writer for the segment.
     *
     * @throws SegmentSealedException if the segment has already been closed.
     */
    @Override
    void close() throws SegmentSealedException;

    /**
     * Returns if the segment is already sealed or not.
     *
     * @return a boolean indicating the seal status of the segment.
     */
    boolean isAlreadySealed();

    /**
     * Gets all events that have been sent to {@link #write(PendingEvent)} but are not yet acknowledged.
     *
     * @return List of events that are sent, but haven't been acked.
     */
    List<PendingEvent<Type>> getUnackedEvents();
}
