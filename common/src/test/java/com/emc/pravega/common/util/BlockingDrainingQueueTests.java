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

package com.emc.pravega.common.util;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.testcommon.AssertExtensions;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for BlockingDrainingQueue class.
 */
public class BlockingDrainingQueueTests {
    private static final int TIMEOUT_MILLIS = 10 * 1000;

    // TODO: write more thorough tests, including for concurrent calls to takeAllEntries.

    /**
     * Tests the basic ability to queue and dequeue items.
     */
    @Test
    public void testQueueDequeue() throws Exception {
        final int itemCount = 10;
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        for (int i = 0; i < itemCount; i++) {
            queue.add(i);
            List<Integer> entries = queue.takeAllEntries().get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            Assert.assertEquals("Unexpected number of items polled.", 1, entries.size());
            int value = entries.get(0);
            Assert.assertEquals("Unexpected value polled from queue.", i, value);
        }
    }

    /**
     * Tests the ability of the queue to block a poll request if it is empty.
     */
    @Test
    public void testBlockingDequeue() throws Exception {
        final int valueToQueue = 1234;

        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        val dequeueFuture = queue.takeAllEntries();

        // Verify the queue hasn't returned before we actually set the result.
        Assert.assertFalse("Queue unblocked before result was set.", dequeueFuture.isDone());

        // Queue the value
        queue.add(valueToQueue);

        // Wait for the completion future to finish. This will also pop any other exceptions that we did not anticipate.
        dequeueFuture.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        // Verify result.
        Assert.assertTrue("Queue did not unblock after adding a value.", FutureHelpers.isSuccessful(dequeueFuture));
        List<Integer> result = dequeueFuture.join();
        Assert.assertEquals("Unexpected number of items polled.", 1, result.size());
        int value = result.get(0);
        Assert.assertEquals("Unexpected value polled from queue.", valueToQueue, value);
    }

    /**
     * Tests the ability of the queue to cancel a polling request if it is closed.
     */
    @Test
    public void testClose() throws Exception {
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        CompletableFuture<List<Integer>> result = queue.takeAllEntries();
        List<Integer> queueContents = queue.close();

        // Verify result.
        AssertExtensions.assertThrows(
                "Future was not cancelled with the correct exception.",
                () -> result.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                ex -> ex instanceof CancellationException);
        Assert.assertEquals("Queue.close() returned an item even though it was empty.", 0, queueContents.size());
    }
}
