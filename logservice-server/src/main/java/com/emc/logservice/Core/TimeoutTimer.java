package com.emc.logservice.Core;

import java.time.Duration;

/**
 * Helps figuring out how much time is left from a particular (initial) timeout.
 */
public class TimeoutTimer {
    private final Duration initialTimeout;
    private final long initialNanos;

    /**
     * Creates a new instane of the TimeoutTimer class.
     *
     * @param initialTimeout The initial timeout.
     */
    public TimeoutTimer(Duration initialTimeout) {
        this.initialTimeout = initialTimeout;
        this.initialNanos = System.nanoTime();
    }

    /**
     * Calculates how much time is left of the original timeout.
     *
     * @return The remaining time.
     */
    public Duration getRemaining() {
        return this.initialTimeout.minusNanos(System.nanoTime() - initialNanos);
    }
}
