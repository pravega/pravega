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
 * IdGenerator is used to generated unique ids which are guaranteed to be unique per process. The id
 * that is generated is 64 bit in length and is composed of
 * ` msb 32 bit = requester ID` and `lsb 32 bit = request ID`.
 *
 * It also provides a method to generate new request ids for a requester id.
 *
 */
public class IdGenerator {

    private static final AtomicLong ID_GENERATOR = new AtomicLong(0);

    /**
     * Get a new requester id.
     * @return id.
     */
    public static long getRequesterId() {
        return ID_GENERATOR.updateAndGet(operand -> {
            int currentRequestor = (int) (operand >> Integer.SIZE);
            return (long) (currentRequestor + 1) << Integer.SIZE;
        });
    }

    /**
     * Return a {@link LongUnaryOperator} that can be used to generate a new Request id.
     * @return id which corresponds to the newer request id.
     */
    public static LongUnaryOperator getRequestId() {
        return id -> {
            int currentRequest = (int) id;
            int currentRequester = (int) (id >> Integer.SIZE);
            return ((long) currentRequester << Integer.SIZE) | ((long) (currentRequest + 1) & 0xFFFFFFFL);
        };
    }

}
