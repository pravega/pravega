/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
