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
package io.pravega.common.util;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.concurrent.Futures;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    /**
     * Tests the basic ability dequeue items using poll() as they are added.
     */
    @Test
    public void testAddSinglePoll() {
        @Cleanup
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();

        for (int i = 0; i < ITEM_COUNT; i++) {
            queue.add(i);
            Assert.assertEquals("Unexpected first element.", i, (int) queue.peek());
            Queue<Integer> entries = queue.poll(MAX_READ_COUNT);
            Assert.assertEquals("Unexpected number of items polled.", 1, entries.size());
            Assert.assertFalse(entries.isEmpty());
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
    public void testAddSingleTake() {
        @Cleanup
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();

        for (int i = 0; i < ITEM_COUNT; i++) {
            queue.add(i);
            val takeResult = queue.take(MAX_READ_COUNT);
            Assert.assertTrue("take() returned an incomplete Future when data is available.", Futures.isSuccessful(takeResult));
            val entries = takeResult.join();
            Assert.assertEquals("Unexpected number of items polled.", 1, entries.size());
            Assert.assertFalse(entries.isEmpty());
            Assert.assertEquals("Unexpected value polled from queue.", i, (int) entries.peek());
        }

        val remainingItems = queue.take(1);
        Assert.assertFalse("take() did not return an incomplete future when queue was empty.", remainingItems.isDone());
    }

    /**
     * Tests the basic ability to dequeue items as a batch using poll().
     */
    @Test
    public void testAddMultiPoll() {
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
    public void testAddMultiTake() {
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
        Assert.assertFalse(result.isEmpty());
        Assert.assertEquals("Unexpected value polled from queue.", valueToQueue, (int) result.peek());

        val remainingItems = queue.poll(MAX_READ_COUNT);
        Assert.assertEquals("Queue was not emptied out after take() completed successfully.", 0, remainingItems.size());
    }

    /**
     * Tests {@link AbstractDrainingQueue#take(int, Duration, ScheduledExecutorService)}.
     */
    @Test
    public void testTakeTimeout() throws Exception {
        final int valueToQueue = 1234;
        final Duration shortTimeout = Duration.ofMillis(10);

        @Cleanup
        BlockingDrainingQueue<Integer> queue = new BlockingDrainingQueue<>();
        val timedoutTake = queue.take(MAX_READ_COUNT, shortTimeout, executorService());
        AssertExtensions.assertSuppliedFutureThrows(
                "take did not time out",
                () -> timedoutTake,
                ex -> ex instanceof TimeoutException);

        val finalTake = queue.take(MAX_READ_COUNT, Duration.ofMillis(TIMEOUT_MILLIS), executorService());
        AssertExtensions.assertThrows(
                "take() succeeded even though there was another incomplete take() request.",
                () -> queue.take(MAX_READ_COUNT),
                ex -> ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "poll() succeeded even though there was another incomplete take() request.",
                () -> queue.poll(MAX_READ_COUNT),
                ex -> ex instanceof IllegalStateException);

        queue.add(valueToQueue);

        val result = finalTake.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        Assert.assertEquals("Unexpected number of items polled.", 1, result.size());
        Assert.assertFalse(result.isEmpty());
        Assert.assertEquals("Unexpected value polled from queue.", valueToQueue, (int) result.peek());

        val remainingItems = queue.poll(MAX_READ_COUNT);
        Assert.assertEquals("Queue was not emptied out after take() completed successfully.", 0, remainingItems.size());
    }

    /**
     * Tests a case where the result from {@link AbstractDrainingQueue#take(int, Duration, ScheduledExecutorService)}
     * is completed concurrently with it timing out.
     */
    @Test
    public void testTakeCompleteTimeoutConcurrently() throws Exception {
        final int valueToQueue = 1234;
        final Duration shortTimeout = Duration.ofMillis(TIMEOUT_MILLIS);

        // We create an InterceptableQueue, which allows us to customize the CompletableFuture returned by take().
        @Cleanup
        InterceptableQueue queue = new InterceptableQueue();

        // We use a special Executor that allows us to intercept calls to schedule().
        @Cleanup("shutdownNow")
        val e = new MockExecutor(1);

        // Register a take() with a timeout (the timeout value is irrelevant for this test).
        InterceptableFuture<Queue<Integer>> takeFuture = (InterceptableFuture<Queue<Integer>>) queue.take(1, shortTimeout, e);

        // Fetch the intercepted calls.
        val timeoutRunnable = e.lastScheduledRunnable;
        Assert.assertNotNull("Unable to intercept the schedule() runnable.", timeoutRunnable);
        Assert.assertNotNull("Unable to intercept take() future creation.", takeFuture);

        // When CompletableFuture.complete() is invoked (from the BlockingDrainingQueue.add() method), we want to
        // immediately invoke the timeout runnable, before we actually complete the CompletableFuture. We want to verify
        // that the timeout won't preempt the normal completion of the future, which would have caused us to lose data.
        takeFuture.completeInterceptor = timeoutRunnable;

        // Add a value, which should trigger the take() future to complete.
        queue.add(valueToQueue);

        // Verify result is as expected.
        val takeResult = takeFuture.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals(1, takeResult.size());
        Assert.assertFalse(takeResult.isEmpty());
        Assert.assertEquals(valueToQueue, (long) takeResult.peek());
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
        val takeResult2Value = takeResult2.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertFalse(takeResult2Value.isEmpty());
        Assert.assertEquals("take() did not work again after being cancelled.", valueToQueue,
                (int) takeResult2Value.poll());
    }

    /**
     * Tests the ability of the queue to cancel a take() request if it is closed.
     */
    @Test
    public void testCloseCancel() {
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
    public void testCloseResult() {
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

    //region Helper classes

    /**
     * {@link ScheduledThreadPoolExecutor} that intercepts any calls to {@link #schedule(Runnable, long, TimeUnit)},
     * captures the {@link Runnable} passed to it and returns a null result.
     */
    private static class MockExecutor extends ScheduledThreadPoolExecutor {
        volatile Runnable lastScheduledRunnable;

        public MockExecutor(int corePoolSize) {
            super(corePoolSize);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            this.lastScheduledRunnable = command;
            return null;
        }
    }

    /**
     * {@link BlockingDrainingQueue} that intercepts the creation of the {@link CompletableFuture} returned by
     * {@link #take} and returns an instance of {@link InterceptableFuture}.
     */
    private static class InterceptableQueue extends BlockingDrainingQueue<Integer> {
        @Override
        @VisibleForTesting
        protected CompletableFuture<Queue<Integer>> newTakeResult() {
            return new InterceptableFuture<>();
        }
    }

    /**
     * {@link CompletableFuture} that allows the interception of the {@link #complete} method.
     */
    private static class InterceptableFuture<T> extends CompletableFuture<T> {
        volatile Runnable completeInterceptor;

        @Override
        public boolean complete(T value) {
            Runnable i = this.completeInterceptor;
            if (i != null) {
                i.run();
            }
            return super.complete(value);
        }
    }

    //endregion
}
