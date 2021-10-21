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

import org.junit.Test;

import static org.junit.Assert.*;

public class ExponentialMovingAverageTest {

    @Test
    public void testSimpleAverages() {
        ExponentialMovingAverage value = new ExponentialMovingAverage(0, .5, false);
        value.addNewSample(10.0);
        assertEquals(5.0, value.getCurrentValue(), .0001);
        value.addNewSample(10.0);
        assertEquals(7.5, value.getCurrentValue(), .0001);
        value.addNewSample(0.0);
        assertEquals(3.75, value.getCurrentValue(), .0001);
        
        value = new ExponentialMovingAverage(0, .1, false);
        value.addNewSample(10.0);
        assertEquals(1.0, value.getCurrentValue(), .0001);
        value.addNewSample(1.0);
        assertEquals(1.0, value.getCurrentValue(), .0001);
        value.addNewSample(0.0);
        assertEquals(0.9, value.getCurrentValue(), .0001);
    }
    
    @Test
    public void testLogWeightedAverages() {
        ExponentialMovingAverage value = new ExponentialMovingAverage(0, .5, true);
        value.addNewSample(10.0);
        assertTrue(value.getCurrentValue() > 1.0);
        assertTrue(value.getCurrentValue() < 5.0);
        value.addNewSample(10.0);
        assertEquals(5.0, value.getCurrentValue(), 1.0);
        value.addNewSample(0.0);
        assertTrue(value.getCurrentValue() > 1.0);
        assertTrue(value.getCurrentValue() < 5.0);
        
        value = new ExponentialMovingAverage(1, .5, true);
        value.addNewSample(-1);
        assertEquals(0.0, value.getCurrentValue(), 0.001);
        value.addNewSample(-10);
        assertTrue(value.getCurrentValue() < 0.0);
        assertTrue(value.getCurrentValue() > -5.0);
    }
    
}
