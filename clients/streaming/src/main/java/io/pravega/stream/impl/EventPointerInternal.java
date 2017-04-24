/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl;

import io.pravega.stream.EventPointer;
import io.pravega.stream.Segment;

/**
 * Pravega provides to a reader the ability to read an isolated event. This feature
 * is useful to enable applications to occasionally perform random reads of events they
 * have already processed. For example, if we index events but store the event bytes
 * in Pravega, then we can go back and use this call to fetch the event bytes.
 *
 * Event pointers are opaque objects. Internally, they map to a segment, start offset
 * pair. It also includes the length for efficient buffering.
 */
public abstract class EventPointerInternal implements EventPointer {

    /**
     * Get the segment object to fetch the event from.
     *
     * @return a segment object
     */
    abstract Segment getSegment();

    /**
     * Get the start offset of the event.
     *
     * @return the start offset for this event
     */
    abstract long getEventStartOffset();

    /**
     * Get the length of the event.
     *
     * @return the event length.
     */
    abstract int getEventLength();
}
