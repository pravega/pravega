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
package io.pravega.client.connection.impl;

import io.pravega.common.AbstractTimer;
import io.pravega.shared.protocol.netty.WireCommands;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AppendBatchSizeTrackerTest {

    
    @Test
    public void testParameterSanity() {
        assertTrue(AppendBatchSizeTrackerImpl.MAX_BATCH_TIME_MILLIS >= 0);
        assertTrue(AppendBatchSizeTrackerImpl.MAX_BATCH_TIME_MILLIS <= 100);
        assertTrue(AppendBatchSizeTrackerImpl.BASE_SIZE >= 0);
        assertTrue(AppendBatchSizeTrackerImpl.BASE_SIZE <= 128 * 1024);
        assertTrue(AppendBatchSizeTrackerImpl.BASE_TIME_NANOS >= 0);
        assertTrue(AppendBatchSizeTrackerImpl.BASE_SIZE <= 20 * AbstractTimer.NANOS_TO_MILLIS);
        assertTrue(AppendBatchSizeTrackerImpl.MAX_BATCH_SIZE >= 0);
        assertTrue(AppendBatchSizeTrackerImpl.MAX_BATCH_SIZE <=  WireCommands.MAX_WIRECOMMAND_SIZE / 2);
        assertTrue(AppendBatchSizeTrackerImpl.OUTSTANDING_FRACTION >= 0);
        assertTrue(AppendBatchSizeTrackerImpl.OUTSTANDING_FRACTION <= 1);
    }
    
    @Test
    public void testInitialValue() {
        AppendBatchSizeTrackerImpl tracker = new AppendBatchSizeTrackerImpl();
        assertEquals(0, tracker.getAppendBlockSize());
    }
    
    @Test
    public void testNonZero() {
        AppendBatchSizeTrackerImpl tracker = new AppendBatchSizeTrackerImpl();
        tracker.recordAppend(1, 100);
        tracker.recordAppend(2, 100);
        assertTrue(tracker.getAppendBlockSize() > 0);
    }
 
    @Test
    public void testMaxSize() {
        int max = WireCommands.MAX_WIRECOMMAND_SIZE / 2;
        AppendBatchSizeTrackerImpl tracker = new AppendBatchSizeTrackerImpl();
        for (int i = 0; i < 100; i++) {
            tracker.recordAppend(i, Integer.MAX_VALUE);
        }
        assertTrue(tracker.getAppendBlockSize() > 0);
        assertTrue(tracker.getAppendBlockSize() <= max);
    }
    
    @Test
    public void testReasonableLowerBound() {
        int size = 2000;
        AppendBatchSizeTrackerImpl tracker = new AppendBatchSizeTrackerImpl();
        for (int i = 0; i < 10000; i++) {
            tracker.recordAppend(i, size);
            if (i > 100) {
                tracker.recordAck(i-100);
            }
        }
        assertTrue(tracker.getAppendBlockSize() >= size * 45);
    }
    
}
