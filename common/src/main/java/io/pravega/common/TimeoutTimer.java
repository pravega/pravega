/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.common;

import java.time.Duration;
import java.util.function.Supplier;
import lombok.ToString;

/**
 * Helps figuring out how much time is left from a particular (initial) timeout.
 */
@ToString(of = { "timeout" })
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
     * Returns a Duration of the time elapsed in Nanoseconds.
     *
     * @return The elapsed time.
     */
    public Duration getElapsed() {
        return Duration.ofNanos(getNanos.get() - initialNanos);
    }

    /**
     * Returns true if there is time remaining.
     *
     * @return False if there is no time remaining from the given timeout.
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
