/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventPointer;
import java.io.ObjectStreamException;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Implementation of the EventPointer interface. We use
 * an EventPointerInternal abstract class as an intermediate
 * class to make pointer instances opaque.
 */
@EqualsAndHashCode(callSuper = false)
public class EventPointerImpl extends EventPointerInternal {
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
    
    public static EventPointer fromString(String eventPointer) {
        int i = eventPointer.lastIndexOf(":");
        Preconditions.checkArgument(i > 0, "Invalid event pointer: %s", eventPointer);
        String[] offset = eventPointer.substring(i + 1).split("-");
        Preconditions.checkArgument(offset.length == 2, "Invalid event pointer: %s", eventPointer);
        return new EventPointerImpl(Segment.fromScopedName(eventPointer.substring(0, i)), Long.parseLong(offset[0]),
                                    Integer.parseInt(offset[1]));
    }
    
    @Override
    public String toString() {
        return segment.getScopedName() + ":" + eventStartOffset + "-" + eventLength;
    }
    
    private Object writeReplace() throws ObjectStreamException {
        return new SerializedForm(toString());
    }
    
    @Data
    private static class SerializedForm  {
        private final String value;
        Object readResolve() throws ObjectStreamException {
            return EventPointer.fromString(value);
        }
    }
}
