/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import io.pravega.common.ObjectClosedException;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for BlockingDrainingQueue class.
 */
public class BlockingDrainingQueueTests extends ThreadPooledTestSuite {
    private static final int ITEM_COUNT = 100;
    private static final int MAX_READ_COUNT = ITEM_COUNT / 10;
    private static final int TIMEOUT_MILLIS = 10 * 1000;
    private static final int BLOCK_VERIFY_TIMEOUT_MILLIS = 10;
    private static final int MAX_SIZE_FOR_BLOCKED_ADDS = 5;

    protected int getThreadPoolSize() {
        return MAX_SIZE_FOR_BLOCKED_ADDS;
    }

    /**
     * Tests the basic ability dequeue items using poll() as they are added.
     */
    @Test
    public void testAddSinglePoll() throws Exception {
        @Cleanup
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();

        for (int i = 0; i < ITEM_COUNT; i++) {
            queue.add(i);
            Queue<Integer> entries = queue.poll(MAX_READ_COUNT);
            Assert.assertEquals("Unexpected number of items polled.", 1, entries.size());
            Assert.assertEquals("Unexpected value polled from queue.", i, (int) entries.peek());
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

    /**
     * Tests the ability of the queue to block pending adds if the queue is full.
     */
    @Test
    public void testMaxCount() throws Exception {
        val expectedItems = new LinkedList<Integer>();
        @Cleanup
        BlockingDrainingQueue<Integer> queue = BlockingDrainingQueue.withMaxCount(MAX_SIZE_FOR_BLOCKED_ADDS);
        for (int i = 0; i < MAX_SIZE_FOR_BLOCKED_ADDS; i++) {
            queue.add(i);
            expectedItems.add(i);
        }

        AtomicInteger nextItem = new AtomicInteger(MAX_SIZE_FOR_BLOCKED_ADDS);
        // Poll 1 item.
        val f1 = queueAndVerifyBlocked(nextItem.incrementAndGet(), queue, expectedItems);
        queue.poll(1);
        expectedItems.removeFirst();
        f1.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected size (poll).", MAX_SIZE_FOR_BLOCKED_ADDS, queue.size());

        // Take 1 item.
        val f2 = queueAndVerifyBlocked(nextItem.incrementAndGet(), queue, expectedItems);
        queue.take(1).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        expectedItems.removeFirst();
        f2.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected size (take).", MAX_SIZE_FOR_BLOCKED_ADDS, queue.size());

        // Closing the queue.
        val fc = queueAndVerifyBlocked(nextItem.incrementAndGet(), queue, expectedItems);
        expectedItems.removeLast(); // This is because we don't expect it to make it there.
        val finalItems = queue.close();
        AssertExtensions.assertThrows(
                "Unexpected exception from blocked call when queue is being closed.",
                () -> fc.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                ex -> ex instanceof ObjectClosedException);

        // Verify that the final contents of the queue is as expected.
        Assert.assertEquals("Unexpected final number of elements in the queue.", expectedItems.size(), finalItems.size());
        val ei = expectedItems.iterator();
        val ai = finalItems.iterator();
        String failMessage = String.format("Unexpected final item in the queue: E={%s}, A={%s}.",
                expectedItems.stream().map(Object::toString).collect(Collectors.joining(",")),
                finalItems.stream().map(Object::toString).collect(Collectors.joining(",")));
        while (ei.hasNext()) {
            val e = ei.next();
            val a = ai.next();
            Assert.assertEquals(failMessage, e, a);
        }
        while (!expectedItems.isEmpty()) {
            val e = expectedItems.poll();
            val a = finalItems.poll();
            Assert.assertEquals("Unexpected final item in the queue.", e, a);
        }
    }

    private CompletableFuture<Void> queueAndVerifyBlocked(int value, BlockingDrainingQueue<Integer> queue, Queue<Integer> expectedItems) {
        val f = CompletableFuture.runAsync(() -> queue.add(value), executorService());
        AssertExtensions.assertThrows(
                "add() did not block synchronously when exceeding capacity.",
                () -> f.get(BLOCK_VERIFY_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                ex -> ex instanceof TimeoutException);
        expectedItems.add(value);
        return f;
    }



    private void populate(BlockingDrainingQueue<Integer> queue) {
        for (int i = 0; i < ITEM_COUNT; i++) {
            queue.add(i);
        }
    }
}
