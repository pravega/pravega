/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.stream.impl;

import io.pravega.client.stream.Segment;

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
