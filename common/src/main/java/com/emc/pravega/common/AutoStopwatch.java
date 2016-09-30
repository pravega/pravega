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
import java.util.function.Supplier;

/**
 * Simple Stopwatch that returns the time since its creation.
 */
public final class AutoStopwatch {
    private final long initialMillis;
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

    /**
     * Gets the elapsed time since the creation of this object.
     *
     * @return The result.
     */
    public Duration elapsed() {
        return Duration.ofMillis(this.getMillis.get() - this.initialMillis);
    }

    @Override
    public String toString() {
        return elapsed().toString();
    }
}
