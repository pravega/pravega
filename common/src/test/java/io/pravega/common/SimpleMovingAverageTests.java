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

import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the SimpleMovingAve
 */
public class SimpleMovingAverageTests {
    private static final double PRECISION = 0.01;
    private static final int ADD_COUNT = 1000;
    private static final int MOVING_COUNT = 10;

    /**
     * Tests the add() and getAverage() methods.
     */
    @Test
    public void testAdd() {
        val sma1 = new SimpleMovingAverage(1);
        val sma10 = new SimpleMovingAverage(MOVING_COUNT);
        Assert.assertEquals("Unexpected SMA(1) when empty.", -1, sma1.getAverage(-1), PRECISION);
        Assert.assertEquals("Unexpected SMA(10) when empty.", -1, sma10.getAverage(-1), PRECISION);
        for (int i = 0; i < ADD_COUNT; i++) {
            sma1.add(i);
            sma10.add(i);
            Assert.assertEquals("Unexpected SMA(1) when adding " + i, i, sma1.getAverage(-1), PRECISION);
            double expectedResult = getExpected(i);
            Assert.assertEquals("Unexpected SMA(10) when adding " + i, expectedResult, sma10.getAverage(-1), PRECISION);
        }
    }

    /**
     * Tests the reset() method.
     */
    @Test
    public void testReset() {
        val sma10 = new SimpleMovingAverage(MOVING_COUNT);
        for (int i = 0; i < MOVING_COUNT; i++) {
            sma10.add(i);
        }
        sma10.reset();
        for (int i = 0; i < MOVING_COUNT; i++) {
            sma10.add(i);
            double expectedResult = getExpected(i);
            Assert.assertEquals("Unexpected SMA(10) when adding " + i, expectedResult, sma10.getAverage(-1), PRECISION);
        }
    }

    private double getExpected(int i) {
        // Expected = Sum(1..i) - Sum(1..i-MOVING_COUNT+1)
        int min = Math.max(0, i - MOVING_COUNT + 1);
        return (i * (i + 1) - min * (min - 1)) / 2.0 / (i - min + 1);
    }
}
