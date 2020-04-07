/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.shared.segment;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Defines Scale Types for Segments.
 */
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum ScaleType {
    /**
     * No scaling.
     */
    NoScaling((byte) 0),
    /**
     * Scale based on the rate in bytes.
     */
    Throughput((byte) 1),
    /**
     * Scale based on the rate in events.
     */
    EventRate((byte) 2);

    @Getter
    private final byte value;

    /**
     * Gets the {@link ScaleType} that matches the given id.
     *
     * @param value The Id to match.
     * @return the {@link ScaleType}.
     */
    public static ScaleType fromValue(byte value) {
        if (value == NoScaling.getValue()) {
            return NoScaling;
        } else if (value == Throughput.getValue()) {
            return Throughput;
        } else if (value == EventRate.getValue()) {
            return EventRate;
        } else {
            throw new IllegalArgumentException("Unsupported Scale Type id " + value);
        }
    }
}