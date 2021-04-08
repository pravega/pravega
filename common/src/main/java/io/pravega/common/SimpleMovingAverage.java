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

import com.google.common.base.Preconditions;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Helps calculate simple moving averages for a number of values. Only the last values added to the series will be included
 * in the calculation.
 */
@NotThreadSafe
public class SimpleMovingAverage {
    private final int[] samples;
    private long sum;
    private int count;
    private int lastIndex;

    /**
     * Creates a new instance of the SimpleMovingAverage class.
     *
     * @param count The maximum number of elements to keep track of.
     */
    public SimpleMovingAverage(int count) {
        Preconditions.checkArgument(count > 0, "count must be a positive integer.");
        this.samples = new int[count];
        reset();
    }

    /**
     * Adds the given value to the moving average. If the moving average is already at capacity, this will overwrite
     * the oldest value in the series.
     *
     * @param value The value to add.
     */
    public void add(int value) {
        int newIndex = (this.lastIndex + 1) % this.samples.length;
        if (this.count >= this.samples.length) {
            // We are going to overwrite a value, so subtract it from the grand sum.
            this.sum -= this.samples[newIndex];
        } else {
            this.count++;
        }

        // Record the new value and update stats.
        this.samples[newIndex] = value;
        this.sum += value;
        this.lastIndex = newIndex;
    }

    /**
     * Clears the SimpleMovingAverage of any data.
     */
    public void reset() {
        // No need to clear the array; we will be overwriting it when we repopulate it anyway.
        this.sum = 0;
        this.count = 0;
        this.lastIndex = -1;
    }

    /**
     * Gets a value indicating the current moving average.
     *
     * @param defaultValue The default value to use.
     * @return The average, or defaultValue if there are no values recorded.
     */
    public double getAverage(double defaultValue) {
        return this.count == 0 ? defaultValue : (double) this.sum / this.count;
    }
}
