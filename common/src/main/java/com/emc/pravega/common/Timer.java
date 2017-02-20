/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common;

import java.time.Duration;

/**
 * Allows easy measurement of elapsed time.
 */
public class Timer {
    private static final int NANOS_TO_MILLIS = 1000 * 1000;
    private volatile long startNanos;

    /**
     * Creates a new instance of the Timer class.
     */
    public Timer() {
        this.startNanos = System.nanoTime();
    }

    /**
     * Resets the time so that zero time has elapsed.
     */
    public void reset() {
        startNanos = System.nanoTime();
    }
    
    /**
     * Gets the elapsed time, in milliseconds, since the creation of this Timer instance.
     */
    public long getElapsedMillis() {
        return getElapsedNanos() / NANOS_TO_MILLIS;
    }

    /**
     * Gets the elapsed time, in nanoseconds, since the creation of this Timer instance.
     */
    public long getElapsedNanos() {
        return Math.max(0, System.nanoTime() - this.startNanos);
    }

    /**
     * Gets the elapsed time since the creation of this Timer instance.
     */
    public Duration getElapsed() {
        return Duration.ofNanos(getElapsedNanos());
    }

    @Override
    public String toString() {
        return getElapsedNanos() + "us";
    }
}