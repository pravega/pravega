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
    void write(PendingEvent<Type> m) throws SegmentSealedException;

    /**
     * Blocks on all outstanding writes.
     *
     * @throws SegmentSealedException If the segment is closed for modifications.
     */
    void flush() throws SegmentSealedException;

    @Override
    void close() throws SegmentSealedException;

    boolean isAlreadySealed();

    /**
     * Gets all events that have been sent to {@link #write(PendingEvent)} but are not yet acknowledged.
     */
    List<PendingEvent<Type>> getUnackedEvents();
}
