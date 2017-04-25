/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl;

import io.pravega.stream.Segment;

import lombok.EqualsAndHashCode;
import lombok.ToString;


/**
 * Implementation of the EventPointer interface. We use
 * an EventPointerInternal abstract class as an intermediate
 * class to make pointer instances opaque.
 */
@EqualsAndHashCode(callSuper = false)
@ToString
public class EventPointerImpl extends EventPointerInternal {
    private static final long serialVersionUID = 1L;
    private final Segment segment;
    private final long eventStartOffset;
    private final int eventLength;

    EventPointerImpl(Segment segment, long eventStartOffset, int eventLength) {
        this.segment = segment;
        this.eventStartOffset = eventStartOffset;
        this.eventLength = eventLength;
    }

    @Override
    Segment getSegment() {
        return segment;
    }

    @Override
    long getEventStartOffset() {
        return eventStartOffset;
    }

    @Override
    int getEventLength() {
        return eventLength;
    }

    @Override
    public EventPointerInternal asImpl() {
        return this;
    }
}
