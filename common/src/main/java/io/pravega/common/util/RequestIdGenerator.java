/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This is used to generated unique request ids per process.
 */
public class RequestIdGenerator {

    private static final AtomicLong REQUEST_ID_GENERATOR = new AtomicLong(0);

    /**
     * Get request id.
     * @return request id.
     */
    public static long getRequestId() {
        return REQUEST_ID_GENERATOR.incrementAndGet();
    }
}
