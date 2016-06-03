package com.emc.logservice.common;

import com.emc.logservice.common.BlockingDrainingQueue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * Unit tests for BlockingDrainingQueue class.
 */
public class BlockingDrainingQueueTests {
    /**
     * Tests the basic ability to queue and dequeue items.
     *
     * @throws Exception
     */
    @Test
    public void testQueueDequeue() throws Exception {
        final int ItemCount = 10;
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();

        for (int i = 0; i < ItemCount; i++) {
            queue.add(i);
            Queue<Integer> entries = queue.takeAllEntries();
            Assert.assertEquals("Unexpected number of items polled.", 1, entries.size());
            int value = entries.poll();
            Assert.assertEquals("Unexpected value polled from queue.", i, value);
        }
    }

    /**
     * Tests the ability of the queue to block a poll request if it is empty.
     *
     * @throws Exception
     */
    @Test
    public void testBlockingDequeue() throws Exception {
        final int WaitTimeoutMillis = 50;
        final int ValueToQueue = 1234;

        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        CompletableFuture<Queue<Integer>> resultFuture = new CompletableFuture<>();

        //Setup a thread to wait for the result to complete.
        Thread waitThread = new Thread(() -> {
            try {
                resultFuture.complete(queue.takeAllEntries());
            }
            catch (InterruptedException ex) {
                resultFuture.completeExceptionally(ex);
            }
        });

        waitThread.start();
        waitThread.join(WaitTimeoutMillis / 2);

        // Verify the queue hasn't returned before we actually set the result.
        Assert.assertFalse("Queue unblocked before result was set.", resultFuture.isDone());

        // Queue the value
        queue.add(ValueToQueue);

        // Wait for the value to be returned.
        waitThread.join(WaitTimeoutMillis);
        if (waitThread.isAlive()) {
            // Thread is still running. Stop it and fail the test.
            waitThread.interrupt();
            Assert.fail("Queue did not unblock within a reasonable amount of time.");
        }

        // Verify result.
        Assert.assertTrue("Queue did not return a value after unblocking.", resultFuture.isDone());
        Queue<Integer> result = resultFuture.join();
        Assert.assertEquals("Unexpected number of items polled.", 1, result.size());
        int value = result.poll();
        Assert.assertEquals("Unexpected value polled from queue.", ValueToQueue, value);
    }

    /**
     * Tests the ability of the queue to cancel a polling request if it is interrupted (via an InterruptedException).
     */
    @Test
    public void testInterruptibility() throws Exception {
        final int WaitTimeoutMillis = 50;
        final int ValueToQueue = 1234;

        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        CompletableFuture<Queue<Integer>> resultFuture = new CompletableFuture<>();

        //Setup a thread to wait for the result to complete.
        Thread waitThread = new Thread(() -> {
            try {
                resultFuture.complete(queue.takeAllEntries());
            }
            catch (InterruptedException ex) {
                resultFuture.completeExceptionally(ex);
            }
        });

        waitThread.start();
        waitThread.join(WaitTimeoutMillis / 2);

        // Verify the queue hasn't returned before we actually set the result.
        Assert.assertFalse("Queue unblocked before result was set.", resultFuture.isDone());

        waitThread.interrupt();

        // Verify result.
        try {
            resultFuture.join();
        }
        catch (CompletionException ex) {
            Assert.assertNotNull("No valid exception was thrown.", ex.getCause());
            Assert.assertTrue("Queue did not throw an InterruptedException.", ex.getCause() instanceof InterruptedException);
            return; // We are done.
        }
        catch (Exception ex) {
            Assert.fail("An unexpected exception was thrown. " + ex);
        }

        Assert.fail("Queue did not throw any exception when interrupted.");
    }
}
