/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest;

import com.google.common.base.Preconditions;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Generates sequential Appends using arbitrary routing keys.
 */
@ThreadSafe
class EventGenerator {
    //region Members

    private final int ownerId;
    private final int routingKeyCount;
    private final boolean recordStartTime;
    private final AtomicInteger sequence;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the EventGenerator class.
     *
     * @param ownerId         The Id to attach to all appends generated with this instance.
     * @param routingKeyCount The number of routing keys to use.
     * @param recordStartTime Whether to record start times in the appends.
     */
    EventGenerator(int ownerId, int routingKeyCount, boolean recordStartTime) {
        Preconditions.checkArgument(routingKeyCount > 0, "routingKeyCount must be a positive integer.");
        this.ownerId = ownerId;
        this.routingKeyCount = routingKeyCount;
        this.recordStartTime = recordStartTime;
        this.sequence = new AtomicInteger(0);
    }

    //endregion

    //region New Event

    /**
     * Generates new Event.
     *
     * @param length The total length of the Event (including header).
     * @return The generated Event.
     */
    Event newEvent(int length) {
        int sequence = this.sequence.getAndIncrement();
        int routingKey = sequence % this.routingKeyCount;
        long startTime = this.recordStartTime ? getCurrentTimeNanos() : Long.MIN_VALUE;
        return new Event(this.ownerId, routingKey, sequence, startTime, length);
    }

    private static long getCurrentTimeNanos() {
        return System.nanoTime();
    }

    //endregion

    //region Validation

    /**
     * Validates that the given Event is valid.
     *
     * @param event The event to inspect.
     * @return A ValidationResult representing the validation.
     */
    static ValidationResult validate(Event event) {
        ValidationResult result;
        try {
            // Deserialize and validate the append.
            event.validateContents();
            result = ValidationResult.success(event.getRoutingKey(), event.getTotalLength());
            if (event.getStartTime() >= 0) {
                // Set the elapsed time, but only for successful operations that did encode the start time.
                result.setElapsed(Duration.ofNanos(getCurrentTimeNanos() - event.getStartTime()));
            }
        } catch (Exception ex) {
            // Any validation exceptions are thrown either from the Event Constructor or from validateContents().
            result = ValidationResult.failed(ex.getMessage());
        }

        return result;
    }

    //endregion
}
