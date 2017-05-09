/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream;

import io.pravega.client.stream.impl.EventPointerInternal;

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
