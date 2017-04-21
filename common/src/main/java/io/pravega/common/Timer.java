/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.common;

/**
 * Allows easy measurement of elapsed time. All elapsed time reported by this class is by reference to the time of
 * this object's creation.
 */
public class Timer extends AbstractTimer {
    private final long startNanos;

    /**
     * Creates a new instance of the Timer class.
     */
    public Timer() {
        this.startNanos = System.nanoTime();
    }

    @Override
    public long getElapsedNanos() {
        return Math.max(0, System.nanoTime() - this.startNanos);
    }
}