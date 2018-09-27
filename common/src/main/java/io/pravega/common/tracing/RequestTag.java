/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.tracing;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@AllArgsConstructor
@EqualsAndHashCode
public class RequestTag {

    public final static long NON_EXISTENT_ID = Long.MIN_VALUE;

    /**
     * The request descriptor serves as an identifier for a request that can be built with the information that methods
     * aiming at accessing to the request tags have already available. This allows multiple internal methods within a
     * component (e.g., segment store, controller) to access the client-generated request id without changing a method's
     * signature. The trade-off comes with the difficulty to discriminate two concurrent client requests with the same
     * descriptor.
     */
    private final String requestDescriptor;

    /**
     * Client-generated id that will enable us to trace the end-to-end lifecycle of a request.
     */
    private final long requestId;

    public boolean isTracked() {
        return requestId != NON_EXISTENT_ID;
    }
}
