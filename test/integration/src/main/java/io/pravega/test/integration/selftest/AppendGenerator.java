/*
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
import io.pravega.common.util.ArrayView;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;

/**
 * Generates sequential Appends using arbitrary routing keys.
 */
@ThreadSafe
class AppendGenerator {
    //region Members

    private final int ownerId;
    private final int routingKeyCount;
    private final boolean recordStartTime;
    private final AtomicInteger sequence;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the AppendGenerator class.
     *
     * @param ownerId         The Id to attach to all appends generated with this instance.
     * @param routingKeyCount The number of routing keys to use.
     * @param recordStartTime Whether to record start times in the appends.
     */
    AppendGenerator(int ownerId, int routingKeyCount, boolean recordStartTime) {
        Preconditions.checkArgument(routingKeyCount > 0, "routingKeyCount must be a positive integer.");
        this.ownerId = ownerId;
        this.routingKeyCount = routingKeyCount;
        this.recordStartTime = recordStartTime;
        this.sequence = new AtomicInteger(0);
    }

    //endregion

    //region New Append

    /**
     * Generates a byte array containing data for an append.
     * Format: [Header][Key][Length][Contents]
     * * [Header]: is a sequence of bytes identifying the start of an append
     * * [Key]: a randomly generated sequence of bytes
     * * [Length]: length of [Contents]
     * * [Contents]: a deterministic result of [Key] & [Length].
     *
     * @param length The total length of the append (including overhead).
     * @return The generated array.
     */
    Append newAppend(int length) {
        int sequence = this.sequence.getAndIncrement();
        int routingKey = sequence % this.routingKeyCount;
        long startTime = this.recordStartTime ? getCurrentTimeNanos() : Long.MIN_VALUE;
        return new Append(this.ownerId, routingKey, sequence, startTime, length);
    }

    private static long getCurrentTimeNanos() {
        return System.nanoTime();
    }

    //endregion

    //region Validation

    /**
     * Validates that the given ArrayView contains a valid Append, starting at the given offset.
     *
     * @param view   The view to inspect.
     * @param offset The offset to start inspecting at.
     * @return A ValidationResult representing the validation.
     */
    static ValidationResult validate(ArrayView view, int offset) {
        ValidationResult result;
        try {
            // Deserialize and validate the append.
            Append a = new Append(view, offset);
            a.validateContents();
            result = ValidationResult.success(a.getRoutingKey(), a.getTotalLength());
            if (a.getStartTime() >= 0) {
                // Set the elapsed time, but only for successful operations that did encode the start time.
                result.setElapsed(Duration.ofNanos(getCurrentTimeNanos() - a.getStartTime()));
            }
        } catch (Exception ex) {
            // Any validation exceptions are thrown either from the Append Constructor or from validateContents().
            result = ValidationResult.failed(ex.getMessage());
        }

        return result;
    }

    //endregion
}
