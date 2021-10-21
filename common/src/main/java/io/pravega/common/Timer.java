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