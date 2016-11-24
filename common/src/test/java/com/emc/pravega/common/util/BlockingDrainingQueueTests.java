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

import com.emc.pravega.testcommon.AssertExtensions;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.CompletableFuture.runAsync;

/**
 * Unit tests for BlockingDrainingQueue class.
 */
public class BlockingDrainingQueueTests {
    private static final int TIMEOUT_MILLIS = 10 * 1000;

    /**
     * Tests the basic ability to queue and dequeue items.
     */
    @Test
    public void testQueueDequeue() throws Exception {
        final int itemCount = 10;
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        for (int i = 0; i < itemCount; i++) {
            queue.add(i);
            List<Integer> entries = queue.takeAllEntries();
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

        AtomicReference<List<Integer>> result = new AtomicReference<>();
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        CompletableFuture<Void> resultSet = new CompletableFuture<>();
        val completionFuture = CompletableFuture.runAsync(() -> {
            try {
                result.set(queue.takeAllEntries());
                resultSet.complete(null);
            } catch (InterruptedException ex) {
                resultSet.completeExceptionally(ex);
            }
        });

        // Verify the queue hasn't returned before we actually set the result.
        Assert.assertNull("Queue unblocked before result was set.", result.get());

        // Queue the value
        queue.add(valueToQueue);

        // Wait for the completion future to finish. This will also pop any other exceptions that we did not anticipate.
        completionFuture.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        resultSet.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        // Verify result.
        Assert.assertNotNull("Queue did not unblock after adding a value.", result.get());
        Assert.assertEquals("Unexpected number of items polled.", 1, result.get().size());
        int value = result.get().get(0);
        Assert.assertEquals("Unexpected value polled from queue.", valueToQueue, value);
    }

    /**
     * Tests the ability of the queue to cancel a polling request if it is closed..
     */
    @Test
    public void testClose() throws Exception {
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        AtomicReference<List<Integer>> result = new AtomicReference<>();
        CompletableFuture<Void> resultSet = new CompletableFuture<>();
        val completionFuture = runAsync(() -> {
            try {
                result.set(queue.takeAllEntries());
                resultSet.complete(null);
            } catch (InterruptedException ex) {
                resultSet.completeExceptionally(ex);
            }
        });

        // Verify the queue hasn't returned before we actually set the result.
        Assert.assertNull("Queue unblocked before result was set.", result.get());
        Thread.sleep(10);
        List<Integer> queueContents = queue.close();

        // Wait for the completion future to finish. This will also pop any other exceptions that we did not anticipate.
        completionFuture.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        // Verify result.
        AssertExtensions.assertThrows(
                "Future was not cancelled with the correct exception.",
                () -> resultSet.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                ex -> ex instanceof InterruptedException);

        Assert.assertNull("Queue returned an item even if it got closed.", result.get());
        Assert.assertEquals("Queue.close() returned an item even though it was empty.", 0, queueContents.size());
    }
}
