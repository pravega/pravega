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

/**
 * Class to store a requestDescriptor, requestId pair in a cache (i.e., RequestTracker) for tracing purposes.
 */
@Data
@AllArgsConstructor
@EqualsAndHashCode
public class RequestTag {

    public final static long NON_EXISTENT_ID = Long.MIN_VALUE;

    /**
     * The request descriptor is the key to access the client-generated requestId.
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
