/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import io.pravega.common.concurrent.Futures;
import io.pravega.test.common.AssertExtensions;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for BlockingDrainingQueue class.
 */
public class BlockingDrainingQueueTests {
    private static final int ITEM_COUNT = 100;
    private static final int MAX_READ_COUNT = ITEM_COUNT / 10;
    private static final int TIMEOUT_MILLIS = 10 * 1000;

    /**
     * Tests the basic ability dequeue items using poll() as they are added.
     */
    @Test
    public void testAddSinglePoll() throws Exception {
        @Cleanup
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();

        for (int i = 0; i < ITEM_COUNT; i++) {
            queue.add(i);
            Assert.assertEquals("Unexpected first element.", i, (int) queue.peek());
            Queue<Integer> entries = queue.poll(MAX_READ_COUNT);
            Assert.assertEquals("Unexpected number of items polled.", 1, entries.size());
            Assert.assertEquals("Unexpected value polled from queue.", i, (int) entries.peek());
            Assert.assertNull("Unexpected first element after removal", queue.peek());
        }

        val remainingItems = queue.poll(1);
        Assert.assertEquals("poll() did not return an empty collection when queue was empty.", 0, remainingItems.size());
    }

    /**
     * Tests the basic ability to dequeue items using take() as they are added.
     */
    @Test
    public void testAddSingleTake() throws Exception {
        @Cleanup
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();

        for (int i = 0; i < ITEM_COUNT; i++) {
            queue.add(i);
            val takeResult = queue.take(MAX_READ_COUNT);
            Assert.assertTrue("take() returned an incomplete Future when data is available.", Futures.isSuccessful(takeResult));
            val entries = takeResult.join();
            Assert.assertEquals("Unexpected number of items polled.", 1, entries.size());
            Assert.assertEquals("Unexpected value polled from queue.", i, (int) entries.peek());
        }

        val remainingItems = queue.take(1);
        Assert.assertFalse("take() did not return an incomplete future when queue was empty.", remainingItems.isDone());
    }

    /**
     * Tests the basic ability to dequeue items as a batch using poll().
     */
    @Test
    public void testAddMultiPoll() throws Exception {
        @Cleanup
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        populate(queue);

        for (int i = 0; i < ITEM_COUNT; i += MAX_READ_COUNT) {
            Queue<Integer> entries = queue.poll(MAX_READ_COUNT);
            int expectedCount = Math.min(MAX_READ_COUNT, ITEM_COUNT - i);
            Assert.assertEquals("Unexpected number of items polled.", expectedCount, entries.size());
            int expectedValue = i;
            for (int value : entries) {
                Assert.assertEquals("Unexpected value polled from queue.", expectedValue, value);
                expectedValue++;
            }
        }
    }

    /**
     * Tests the basic ability to dequeue items as a batch using take().
     */
    @Test
    public void testAddMultiTake() throws Exception {
        @Cleanup
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        populate(queue);

        for (int i = 0; i < ITEM_COUNT; i += MAX_READ_COUNT) {
            val takeResult = queue.take(MAX_READ_COUNT);
            Assert.assertTrue("take() returned an incomplete Future when data is available.", Futures.isSuccessful(takeResult));
            val entries = takeResult.join();

            int expectedCount = Math.min(MAX_READ_COUNT, ITEM_COUNT - i);
            Assert.assertEquals("Unexpected number of items polled.", expectedCount, entries.size());
            int expectedValue = i;
            for (int value : entries) {
                Assert.assertEquals("Unexpected value polled from queue.", expectedValue, value);
                expectedValue++;
            }
        }
    }

    /**
     * Tests the ability of the queue to fulfill a take() request if it is empty when it was received.
     */
    @Test
    public void testBlockingTake() throws Exception {
        final int valueToQueue = 1234;

        @Cleanup
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        val takeResult = queue.take(MAX_READ_COUNT);

        // Verify we cannot have multiple concurrent take() or poll() requests.
        AssertExtensions.assertThrows(
                "take() succeeded even though there was another incomplete take() request.",
                () -> queue.take(MAX_READ_COUNT),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "poll() succeeded even though there was another incomplete take() request.",
                () -> queue.poll(MAX_READ_COUNT),
                ex -> ex instanceof IllegalStateException);

        // Verify the queue hasn't returned before we actually set the result.
        Assert.assertFalse("Queue unblocked before result was set.", takeResult.isDone());

        // Queue the value
        queue.add(valueToQueue);

        // Wait for the completion future to finish. This will also pop any other exceptions that we did not anticipate.
        takeResult.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        // Verify result.
        Assert.assertTrue("Queue did not unblock after adding a value.", Futures.isSuccessful(takeResult));
        Queue<Integer> result = takeResult.join();
        Assert.assertEquals("Unexpected number of items polled.", 1, result.size());
        Assert.assertEquals("Unexpected value polled from queue.", valueToQueue, (int) result.peek());

        val remainingItems = queue.poll(MAX_READ_COUNT);
        Assert.assertEquals("Queue was not emptied out after take() completed successfully.", 0, remainingItems.size());
    }

    /**
     * Tests the ability to cancel a pending take() operation.
     */
    @Test
    public void testCancelPendingTake() throws Exception {
        final int valueToQueue = 1234;

        @Cleanup
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        val takeResult = queue.take(MAX_READ_COUNT);

        Assert.assertFalse("take() returned a completed future.", takeResult.isDone());
        queue.cancelPendingTake();
        Assert.assertTrue("cancelPendingTake() did not cancel a pending take() future.", takeResult.isCancelled());

        val takeResult2 = queue.take(MAX_READ_COUNT);
        queue.add(valueToQueue);
        Assert.assertEquals("take() did not work again after being cancelled.", valueToQueue,
                (int) takeResult2.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS).poll());
    }

    /**
     * Tests the ability of the queue to cancel a take() request if it is closed.
     */
    @Test
    public void testCloseCancel() throws Exception {
        @Cleanup
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        CompletableFuture<Queue<Integer>> result = queue.take(MAX_READ_COUNT);
        Collection<Integer> queueContents = queue.close();

        // Verify result.
        AssertExtensions.assertThrows(
                "Future was not cancelled with the correct exception.",
                () -> result.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                ex -> ex instanceof CancellationException);
        Assert.assertEquals("Queue.close() returned an item even though it was empty.", 0, queueContents.size());
    }

    /**
     * Tests the ability of the queue to return its contents when it is closed.
     */
    @Test
    public void testCloseResult() throws Exception {
        @Cleanup
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        populate(queue);

        Collection<Integer> queueContents = queue.close();
        // Verify result.
        Assert.assertEquals("Unexpected result size from Queue.close().", ITEM_COUNT, queueContents.size());
        int expectedValue = 0;
        for (int value : queueContents) {
            Assert.assertEquals("Unexpected value in Queue result.", expectedValue, value);
            expectedValue++;
        }
    }

    private void populate(BlockingDrainingQueue<Integer> queue) {
        for (int i = 0; i < ITEM_COUNT; i++) {
            queue.add(i);
        }
    }
}
