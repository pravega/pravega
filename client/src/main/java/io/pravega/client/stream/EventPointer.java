/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import io.pravega.client.stream.impl.EventPointerInternal;
import java.nio.ByteBuffer;

/**
 * A pointer to an event. This can be used to retrieve a previously read event by calling {@link EventStreamReader#fetchEvent(EventPointer)}
 */
public interface EventPointer {

    /**
     * Used internally. Do not call.
     *
     * @return Implementation of EventPointer interface
     */
    EventPointerInternal asImpl();
    
    /**
     * Serializes the Event pointer to a compact binary form.
     * @return A binary representation of the event pointer.
     */
    ByteBuffer toBytes();

    /**
     * Deserializes the event pointer from its serialized from obtained from calling {@link #toBytes()}.
     * 
     * @param eventPointer A serialized event pointer.
     * @return The event pointer object.
     */
    static EventPointer fromBytes(ByteBuffer eventPointer) {
        return EventPointerInternal.fromBytes(eventPointer);
    }
    
}
