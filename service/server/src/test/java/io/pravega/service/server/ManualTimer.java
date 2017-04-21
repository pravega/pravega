/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server;

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
