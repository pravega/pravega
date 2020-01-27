/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.common.AbstractTimer;
import lombok.Getter;

/**
 * AbstractTimer Implementation that allows full control over the return values, for use in Unit testing.
 */
public class ManualTimer extends AbstractTimer {
    @Getter
    private volatile long elapsedNanos;

    /**
     * Sets the value of the timer.
     *
     * @param value The value to set, in milliseconds.
     */
    public void setElapsedMillis(long value) {
        this.elapsedNanos = value * NANOS_TO_MILLIS;
    }
}
