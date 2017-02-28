/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Helps figuring out how much time is left from a particular (initial) timeout.
 */
public class TimeoutTimer {
    private final Supplier<Long> getNanos;
    private volatile Duration timeout;
    private volatile long initialNanos;

    /**
     * Creates a new instance of the TimeoutTimer class.
     *
     * @param initialTimeout The initial timeout.
     */
    public TimeoutTimer(Duration initialTimeout) {
        this(initialTimeout, System::nanoTime);
    }
    
    /**
     * Creates a new instance of the TimeoutTimer class.
     *
     * @param initialTimeout The initial timeout.
     * @param getNanos The supplier of nanoseconds.
     */
    public TimeoutTimer(Duration initialTimeout, Supplier<Long> getNanos) {
        this.timeout = initialTimeout;
        this.getNanos = getNanos;
        this.initialNanos = getNanos.get();
    }

    /**
     * Calculates how much time is left of the original timeout.
     *
     * @return The remaining time.
     */
    public Duration getRemaining() {
        return timeout.minusNanos(getNanos.get() - initialNanos);
    }
    
    /**
     * Returns true if there is time remaining.
     */
    public boolean hasRemaining() {
        return (getNanos.get() - initialNanos) < timeout.toNanos();
    }
    
    /**
     * Reset the timeout so that the original amount of time is remaining. While it is safe to call
     * this concurrently with {@link #getRemaining()}, the value returned by {@link #getRemaining()}
     * may be wrong. A synchronized block is NOT required to avoid this.
     * 
     * @param timeout The duration from now which should be placed on the timer.
     */
    public void reset(Duration timeout) {
        this.initialNanos = getNanos.get();
        this.timeout = timeout;
    }
    
    /**
     * Adjust the time so that the is no time remaining.
     */
    public void zero() {
        this.initialNanos = getNanos.get() - timeout.toNanos();
    }
}
