/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream;

import io.pravega.stream.impl.EventPointerInternal;

import java.io.Serializable;

/**
 * A pointer to an event. This can be used to retrieve a previously read event by calling {@link EventStreamReader#read(EventPointer)}
 */
public interface EventPointer extends Serializable {

    /**
     * Used internally. Do not call.
     *
     * @return Implementation of EventPointer interface
     */
    EventPointerInternal asImpl();

}
