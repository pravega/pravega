package com.emc.logservice.common;

import com.emc.nautilus.testcommon.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

/**
 * Unit tests for BlockingDrainingQueue class.
 */
public class BlockingDrainingQueueTests {
    /**
     * Tests the basic ability to queue and dequeue items.
     */
    @Test
    public void testQueueDequeue() throws Exception {
        final int ItemCount = 10;
        try (BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>()) {
            for (int i = 0; i < ItemCount; i++) {
                queue.add(i);
                Queue<Integer> entries = queue.takeAllEntries().join();
                Assert.assertEquals("Unexpected number of items polled.", 1, entries.size());
                int value = entries.poll();
                Assert.assertEquals("Unexpected value polled from queue.", i, value);
            }
        }
    }

    /**
     * Tests the ability of the queue to block a poll request if it is empty.
     */
    @Test
    public void testBlockingDequeue() throws Exception {
        final int ValueToQueue = 1234;

        try (BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>()) {
            CompletableFuture<Queue<Integer>> resultFuture = queue.takeAllEntries();

            // Verify the queue hasn't returned before we actually set the result.
            Assert.assertFalse("Queue unblocked before result was set.", resultFuture.isDone());

            // Queue the value
            queue.add(ValueToQueue);

            // Verify result.
            Assert.assertTrue("Queue did unblock after adding a value.", resultFuture.isDone());
            Queue<Integer> result = resultFuture.join();
            Assert.assertEquals("Unexpected number of items polled.", 1, result.size());
            int value = result.poll();
            Assert.assertEquals("Unexpected value polled from queue.", ValueToQueue, value);
        }
    }

    /**
     * Tests the ability of the queue to handle an external cancellation of a call to takeAllEntries.
     */
    @Test
    public void testCancellation() throws Exception {
        final int ValueToQueue = 1234;

        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        CompletableFuture<Queue<Integer>> resultFuture = queue.takeAllEntries();

        // Verify the queue hasn't returned before we actually set the result.
        Assert.assertFalse("Queue unblocked before result was set.", resultFuture.isDone());

        // Check that we cannot have more than one concurrent request to takeAllEntries
        AssertExtensions.assertThrows(
                "takeAllEntries allowed a concurrent request.",
                queue::takeAllEntries,
                ex -> ex instanceof IllegalStateException);

        resultFuture.cancel(true);

        Assert.assertTrue("Future was not cancelled.", resultFuture.isCompletedExceptionally());
        AssertExtensions.assertThrows(
                "Future was not cancelled with the correct exception.",
                resultFuture::join,
                ex -> ex instanceof CancellationException);

        resultFuture = queue.takeAllEntries();
        queue.add(ValueToQueue);

        // Verify result.
        Assert.assertTrue("Queue did unblock after adding a value.", resultFuture.isDone());
        Queue<Integer> result = resultFuture.join();
        Assert.assertEquals("Unexpected number of items polled.", 1, result.size());
        int value = result.poll();
        Assert.assertEquals("Unexpected value polled from queue.", ValueToQueue, value);
    }

    /**
     * Tests the ability of the queue to cancel a polling request if it is closed..
     */
    @Test
    public void testClose() throws Exception {
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        CompletableFuture<Queue<Integer>> resultFuture = queue.takeAllEntries();

        // Verify the queue hasn't returned before we actually set the result.
        Assert.assertFalse("Queue unblocked before result was set.", resultFuture.isDone());

        queue.close();

        // Verify result.
        AssertExtensions.assertThrows(
                "Future was not cancelled with the correct exception.",
                resultFuture::join,
                ex -> ex instanceof CancellationException);
    }
}
