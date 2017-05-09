/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.Segment;
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
