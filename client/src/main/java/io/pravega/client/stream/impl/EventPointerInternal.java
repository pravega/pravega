/**
 * Copyright Pravega Authors.
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

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.EventPointer;
import java.nio.ByteBuffer;

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
    
    public static EventPointer fromBytes(ByteBuffer eventPointer) {
        return EventPointerImpl.fromBytes(eventPointer);
    }
}
