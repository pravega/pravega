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
import java.util.function.LongUnaryOperator;

/**
 * This is used to generated unique requester ids which is guranteed to be unique per process.
 * It also provides a method to generate new request ids from a requester id.
 */
public class IdGenerator {

    private static final AtomicLong ID_GENERATOR = new AtomicLong(0);

    /**
     * Get the requester id.
     * @return request id.
     */
    public static long getRequesterId() {
        return ID_GENERATOR.updateAndGet(operand -> {
            int currentRequestor = (int) (operand >> Integer.SIZE);
            return (long) (currentRequestor + 1) << Integer.SIZE;
        });
    }

    /**
     * Return a {@link LongUnaryOperator} that can be used to generate a new Request id.
     * @return
     */
    public static LongUnaryOperator getRequestId() {
        return id -> {
            int currentRequest = (int) id;
            int currentRequester = (int) (id >> Integer.SIZE);
            return ((long) currentRequester << Integer.SIZE) | ((long) (currentRequest + 1) & 0xFFFFFFFL);
        };
    }

}
