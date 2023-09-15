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
package io.pravega.common.concurrent;

import io.pravega.common.AbstractTimer;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link DelayedProcessor} class.
 */
public class DelayedProcessorTests extends ThreadPooledTestSuite {
    private static final Duration DEFAULT_DELAY = Duration.ofMillis(1000);
    private static final Duration SEPARATION_DELAY = Duration.ofMillis(100);
    private static final int TIMEOUT_MILLIS = 30000;

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests {@link DelayedProcessor#process} when no duplication or cancellation occurs.
     */
    @Test(timeout = 5000)
    public void testProcessNormal() throws Exception {
        val processedItems = Collections.synchronizedList(new ArrayList<>());
        val currentProcessFuture = new AtomicReference<CompletableFuture<Void>>();
        Function<TestItem, CompletableFuture<Void>> itemProcessor = item -> {
            processedItems.add(item);
            val r = new CompletableFuture<Void>();
            currentProcessFuture.set(r);
            return r;
        };
        @Cleanup("shutdown")
        val p = new TestProcessor(itemProcessor);

        val i1 = new TestItem("1");
        val i2 = new TestItem("2");
        val i3 = new TestItem("3");

        // Queue item #1.
        p.process(i1);

        // Force an early iteration - verify the item has not yet been processed.
        p.advanceTime(SEPARATION_DELAY);
        p.awaitNewIteration();
        p.releaseDelayedFuture();
        p.awaitNewIteration();
        Assert.assertEquals("Unexpected delay requested after early release.", DEFAULT_DELAY.minus(SEPARATION_DELAY), p.lastRequestedDelay);

        // Queue item #2 and a bit of time later, #3.
        p.process(i2);
        p.advanceTime(SEPARATION_DELAY);
        p.process(i3);
        Assert.assertEquals(3, p.size());

        // Properly advance the time, and verify we only process #1.
        p.advanceTime(DEFAULT_DELAY.minus(SEPARATION_DELAY).minus(SEPARATION_DELAY));
        Assert.assertEquals("Not expecting any processed items yet.", 0, processedItems.size());
        p.releaseDelayedFuture();
        AssertExtensions.assertEventuallyEquals(1, processedItems::size, TIMEOUT_MILLIS);
        Assert.assertNotNull(currentProcessFuture.get());
        Assert.assertNull(p.delayedFuture); // Make sure we haven't yet begun another iteration.
        Assert.assertEquals("Not expected processing item to be dequeued.", 3, p.size());

        // Complete processing of item #1
        currentProcessFuture.getAndSet(null).complete(null);

        // Now we want to process items #2 and #3 in short sequence.
        p.awaitNewIteration();
        Assert.assertEquals("Expected processed item to be dequeued.", 2, p.size());
        p.advanceTime(SEPARATION_DELAY.plus(SEPARATION_DELAY));
        p.releaseDelayedFuture();
        AssertExtensions.assertEventuallyEquals(2, processedItems::size, TIMEOUT_MILLIS);
        Assert.assertNotNull(currentProcessFuture.get());
        Assert.assertNull(p.delayedFuture); // Make sure we haven't yet begun another iteration.
        Assert.assertEquals("Not expected processing item to be dequeued.", 2, p.size());

        // Complete processing of item #2
        currentProcessFuture.getAndSet(null).complete(null);
        p.awaitNewIteration();
        Assert.assertEquals("Not expecting a delay for item 3.", 0, p.lastRequestedDelay.toMillis());
        Assert.assertEquals("Expected processed item to be dequeued.", 1, p.size());
        p.releaseDelayedFuture();
        AssertExtensions.assertEventuallyEquals(3, processedItems::size, TIMEOUT_MILLIS);
        Assert.assertNotNull(currentProcessFuture.get());
        Assert.assertNull(p.delayedFuture); // Make sure we haven't yet begun another iteration.
        Assert.assertEquals("Not expected processing item to be dequeued.", 1, p.size());
        currentProcessFuture.getAndSet(null).complete(null);

        // Final verification.
        p.awaitNewIteration();
        Assert.assertEquals("Expected all processed items to be dequeued.", 0, p.size());
        Assert.assertEquals("Expected default delay between iterations when no items left.",
                DEFAULT_DELAY.toMillis(), p.lastRequestedDelay.toMillis());
        AssertExtensions.assertListEquals("Unexpected items processed.", Arrays.asList(i1, i2, i3), processedItems, Object::equals);
    }

    /**
     * Tests {@link DelayedProcessor#process} when duplicated items are added.
     */
    @Test(timeout = 5000)
    public void testProcessDuplicated() throws Exception {
        val processedItems = Collections.synchronizedList(new ArrayList<>());
        val currentProcessFuture = new AtomicReference<CompletableFuture<Void>>();
        Function<TestItem, CompletableFuture<Void>> itemProcessor = item -> {
            processedItems.add(item);
            val r = new CompletableFuture<Void>();
            currentProcessFuture.set(r);
            return r;
        };
        @Cleanup("shutdown")
        val p = new TestProcessor(itemProcessor);

        val i1 = new TestItem("1");
        val i2 = new TestItem("2");
        val i3 = new TestItem(i1.key()); // i1 and i3 refer to the same items.

        // Queue item #1.
        p.process(i1);
        p.advanceTime(SEPARATION_DELAY);
        p.process(i2);
        p.advanceTime(SEPARATION_DELAY);
        p.process(i3);
        Assert.assertEquals("Unexpected number of items queued.", 2, p.size());

        // Advance the time, and verify we only process #1 and #2 (#3 should be ignored).
        p.awaitNewIteration();
        p.advanceTime(DEFAULT_DELAY.plus(DEFAULT_DELAY));
        p.releaseDelayedFuture();

        // Await and complete processing of item #1.
        AssertExtensions.assertEventuallyEquals(1, processedItems::size, TIMEOUT_MILLIS);
        currentProcessFuture.getAndSet(null).complete(null);

        p.awaitNewIteration();
        Assert.assertEquals(0, p.lastRequestedDelay.toMillis());
        p.releaseDelayedFuture();

        // Await and complete processing of item #2
        AssertExtensions.assertEventuallyEquals(2, processedItems::size, TIMEOUT_MILLIS);
        currentProcessFuture.getAndSet(null).complete(null);

        // Final verification.
        p.awaitNewIteration();
        Assert.assertEquals("Expected all processed items to be dequeued.", 0, p.size());
        Assert.assertEquals("Expected default delay between iterations when no items left.",
                DEFAULT_DELAY.toMillis(), p.lastRequestedDelay.toMillis());
        AssertExtensions.assertListEquals("Unexpected items processed.", Arrays.asList(i1, i2), processedItems, Object::equals);
    }

    /**
     * Tests the ability to handle processing errors from external handlers.
     */
    @Test(timeout = 5000)
    public void testProcessWithErrors() throws Exception {
        val processedItems = Collections.synchronizedList(new ArrayList<>());
        val currentProcessFuture = new AtomicReference<CompletableFuture<Void>>();
        Function<TestItem, CompletableFuture<Void>> itemProcessor = item -> {
            processedItems.add(item);
            val r = new CompletableFuture<Void>();
            currentProcessFuture.set(r);
            return r;
        };
        @Cleanup("shutdown")
        val p = new TestProcessor(itemProcessor);

        val i1 = new TestItem("1");
        val i2 = new TestItem("2");

        // Queue item #1.
        p.process(i1);
        p.advanceTime(SEPARATION_DELAY);
        p.process(i2);
        Assert.assertEquals("Unexpected number of items queued.", 2, p.size());

        // Advance the time, and verify we only process #1.
        p.awaitNewIteration();
        p.advanceTime(DEFAULT_DELAY.plus(DEFAULT_DELAY));
        p.releaseDelayedFuture();

        // Await and complete processing of item #1. Also cancel #2 while this is happening.
        AssertExtensions.assertEventuallyEquals(1, processedItems::size, TIMEOUT_MILLIS);
        currentProcessFuture.getAndSet(null).completeExceptionally(new IntentionalException());

        p.awaitNewIteration();
        Assert.assertEquals("Unexpected number of items queued after failure.", 1, p.size());
        p.releaseDelayedFuture();
        AssertExtensions.assertEventuallyEquals(2, processedItems::size, TIMEOUT_MILLIS);
        currentProcessFuture.getAndSet(null).complete(null);

        // Final verification.
        p.awaitNewIteration();
        Assert.assertEquals("Expected all processed items to be dequeued.", 0, p.size());
        Assert.assertEquals("Expected default delay between iterations when no items left.",
                DEFAULT_DELAY.toMillis(), p.lastRequestedDelay.toMillis());
        AssertExtensions.assertListEquals("Unexpected items processed.", Arrays.asList(i1, i2), processedItems, Object::equals);
    }

    /**
     * Tests the ability to cancel items using {@link DelayedProcessor#cancel} before they start executing.
     */
    @Test(timeout = 5000)
    public void testCancel() throws Exception {
        val processedItems = Collections.synchronizedList(new ArrayList<>());
        val currentProcessFuture = new AtomicReference<CompletableFuture<Void>>();
        Function<TestItem, CompletableFuture<Void>> itemProcessor = item -> {
            processedItems.add(item);
            val r = new CompletableFuture<Void>();
            currentProcessFuture.set(r);
            return r;
        };
        @Cleanup("shutdown")
        val p = new TestProcessor(itemProcessor);

        val i1 = new TestItem("1");
        val i2 = new TestItem("2");

        // Queue item #1.
        p.process(i1);
        p.advanceTime(SEPARATION_DELAY);
        p.process(i2);
        Assert.assertEquals("Unexpected number of items queued.", 2, p.size());

        // Advance the time, and verify we only process #1.
        p.awaitNewIteration();
        p.advanceTime(DEFAULT_DELAY.plus(DEFAULT_DELAY));
        p.releaseDelayedFuture();

        // Await and complete processing of item #1. Also cancel #2 while this is happening.
        AssertExtensions.assertEventuallyEquals(1, processedItems::size, TIMEOUT_MILLIS);
        p.cancel(i2.key());
        Assert.assertEquals("Unexpected number of items queued after cancellation.", 1, p.size());
        currentProcessFuture.getAndSet(null).complete(null);

        // Final verification.
        p.awaitNewIteration();
        Assert.assertEquals("Expected all processed items to be dequeued.", 0, p.size());
        Assert.assertEquals("Expected default delay between iterations when no items left.",
                DEFAULT_DELAY.toMillis(), p.lastRequestedDelay.toMillis());
        AssertExtensions.assertListEquals("Unexpected items processed.", Collections.singletonList(i1), processedItems, Object::equals);
    }

    /**
     * Tests the ability to cancel items using {@link DelayedProcessor#cancel} while they are processing. This should have
     * no effect, however it should not cause the processor to skip other items or otherwise function unexpectedly.
     */
    @Test(timeout = 5000)
    public void testCancelWhileProcessing() throws Exception {
        val processedItems = Collections.synchronizedList(new ArrayList<>());
        val currentProcessFuture = new AtomicReference<CompletableFuture<Void>>();
        Function<TestItem, CompletableFuture<Void>> itemProcessor = item -> {
            processedItems.add(item);
            val r = new CompletableFuture<Void>();
            currentProcessFuture.set(r);
            return r;
        };
        @Cleanup("shutdown")
        val p = new TestProcessor(itemProcessor);

        val i1 = new TestItem("1");
        val i2 = new TestItem("2");

        // Queue item #1.
        p.process(i1);
        p.advanceTime(SEPARATION_DELAY);
        p.process(i2);
        Assert.assertEquals("Unexpected number of items queued.", 2, p.size());

        // Advance the time, and verify we only process #1.
        p.awaitNewIteration();
        p.advanceTime(DEFAULT_DELAY.plus(DEFAULT_DELAY));
        p.releaseDelayedFuture();

        // Await and complete processing of item #1. While this is happening, cancel it.
        AssertExtensions.assertEventuallyEquals(1, processedItems::size, TIMEOUT_MILLIS);
        p.cancel(i1.key());
        Assert.assertEquals("Unexpected number of items queued after cancellation.", 1, p.size());
        currentProcessFuture.getAndSet(null).complete(null);

        // Now let #2 get processed as well.
        p.awaitNewIteration();
        p.releaseDelayedFuture();
        AssertExtensions.assertEventuallyEquals(2, processedItems::size, TIMEOUT_MILLIS);
        currentProcessFuture.getAndSet(null).complete(null);

        // Final verification.
        p.awaitNewIteration();
        Assert.assertEquals("Expected all processed items to be dequeued.", 0, p.size());
        Assert.assertEquals("Expected default delay between iterations when no items left.",
                DEFAULT_DELAY.toMillis(), p.lastRequestedDelay.toMillis());
        AssertExtensions.assertListEquals("Unexpected items processed.", Arrays.asList(i1, i2), processedItems, Object::equals);
    }

    /**
     * Tests {@link DelayedProcessor#close()} and its ability to immediately shut down the processor.
     */
    @Test(timeout = 5000)
    public void testClose() throws Exception {
        val processedItemCount = new AtomicInteger(0);
        val currentProcessFuture = new AtomicReference<CompletableFuture<Void>>();
        Function<TestItem, CompletableFuture<Void>> itemProcessor = item -> {
            processedItemCount.incrementAndGet();
            val r = new CompletableFuture<Void>();
            currentProcessFuture.set(r);
            return r;
        };

        val i1 = new TestItem("1");

        // First test is when we cancel while delaying for next iteration.
        val p1 = new TestProcessor(itemProcessor);
        p1.awaitNewIteration();
        p1.process(i1);
        p1.advanceTime(DEFAULT_DELAY);
        p1.shutdown();
        p1.releaseDelayedFuture();
        Assert.assertEquals("Not expecting any items to be processed when closed while waiting.", 0, processedItemCount.get());

        // Second test is when we cancel while executing next iteration.
        val p2 = new TestProcessor(itemProcessor);
        p2.process(i1);
        p2.awaitNewIteration();
        p2.advanceTime(DEFAULT_DELAY);
        p2.releaseDelayedFuture();
        AssertExtensions.assertEventuallyEquals(1, processedItemCount::get, TIMEOUT_MILLIS);
        p1.shutdown();
        currentProcessFuture.getAndSet(null).complete(null);

        Assert.assertEquals("Unexpected number of items processed.", 1, processedItemCount.get());
    }

    private class TestProcessor extends DelayedProcessor<TestItem> {
        @Getter
        private final ManualTimer timer = new ManualTimer();
        private volatile CompletableFuture<Void> delayedFuture;
        private volatile Duration lastRequestedDelay;

        TestProcessor(Function<TestItem, CompletableFuture<Void>> itemProcessor) {
            super(itemProcessor, DEFAULT_DELAY, executorService(), "Test");
        }

        @Override
        protected synchronized CompletableFuture<Void> createDelayedFuture(Duration delay) {
            Assert.assertNull(this.delayedFuture);
            this.lastRequestedDelay = delay;
            this.delayedFuture = new CompletableFuture<>();
            return this.delayedFuture;
        }

        void awaitNewIteration() throws Exception {
            TestUtils.await(() -> this.delayedFuture != null, 10, TIMEOUT_MILLIS);
        }

        void advanceTime(Duration duration) {
            advanceTime((int) duration.toMillis());
        }

        void advanceTime(int millis) {
            this.timer.setElapsedMillis(timer.getElapsedMillis() + millis);
        }

        void releaseDelayedFuture() {
            val d = this.delayedFuture;
            this.delayedFuture = null;
            this.lastRequestedDelay = null;
            d.complete(null);
        }
    }

    @RequiredArgsConstructor
    private static class TestItem implements DelayedProcessor.Item {
        private final String key;

        @Override
        public String key() {
            return this.key;
        }
    }

    private static class ManualTimer extends AbstractTimer {
        @Getter
        private volatile long elapsedNanos;

        void setElapsedMillis(long value) {
            this.elapsedNanos = value * NANOS_TO_MILLIS;
        }
    }
}
