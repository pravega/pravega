/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Simple Stopwatch that returns the time since its creation. This class is not susceptible to system clock changes
 * and its usage should be preferred to doing Date arithmetic using the current system time.
 */
public final class AutoStopwatch {
    private volatile long initialMillis;
    private final Supplier<Long> getMillis;

    /**
     * Creates a new instance of the AutoStopwatch class, using System.currentTimeMillis() as time provider.
     */
    public AutoStopwatch() {
        this(System::currentTimeMillis);
    }

    /**
     * Creates a new instance of the AutoStopwatch class with the given supplier of the current time (in milliseconds).
     *
     * @param getMillis The supplier of milliseconds.
     */
    public AutoStopwatch(Supplier<Long> getMillis) {
        this.getMillis = getMillis;
        this.initialMillis = getMillis.get();
    }

    public void reset() {
       initialMillis = getMillis.get();
    }
    
    /**
     * Gets the elapsed time since the creation of this object.
     */
    public Duration elapsed() {
        return Duration.ofMillis(Math.max(0, this.getMillis.get() - this.initialMillis));
    }

    @Override
    public String toString() {
        return elapsed().toString();
    }
}
