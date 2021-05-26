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

import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.Getter;
import lombok.Setter;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the {@link OrderedProcessor} class.
 */
public class OrderedProcessorTests extends ThreadPooledTestSuite {
    private static final int CAPACITY = 10;
    private static final Function<Integer, Integer> TRANSFORMER = i -> i + 1;

    @Rule
    public Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests a scenario where we add fewer items than the capacity allows. We want to verify that none of the items are
     * queued up.
     */
    @Test
    public void testCapacityNotExceeded() throws Exception {
        val processedItems = Collections.synchronizedCollection(new HashSet<Integer>());
        val processFutures = Collections.synchronizedList(new ArrayList<CompletableFuture<Integer>>());
        Function<Integer, CompletableFuture<Integer>> itemProcessor = i -> {
            if (!processedItems.add(i)) {
                Assert.fail("Duplicate item detected: " + i);
            }

            CompletableFuture<Integer> result = new CompletableFuture<>();
            processFutures.add(result);
            return result;
        };

        val resultFutures = new ArrayList<CompletableFuture<Integer>>();
        @Cleanup
        val p = new TestProcessor(CAPACITY, executorService());
        for (int i = 0; i < CAPACITY; i++) {
            val item = i;
            resultFutures.add(p.execute(() -> itemProcessor.apply(item)));
            Assert.assertTrue("Item has not been immediately processed when under capacity: " + i, processedItems.contains(item));
        }

        // Finish up half the futures. We need a Semaphore so that we know when the OrderedProcessor actually
        // finished cleaning up after these completed tasks, as that happens asynchronously inside the processor and we
        // don't really have a hook into it, except by sub-classing it and intercepting 'executionComplete'.
        val half = CAPACITY / 2;
        val cleanupSignal = new Semaphore(half);
        cleanupSignal.acquire(half);
        p.setExecutionCompleteCallback(cleanupSignal::release);
        for (int i = 0; i < half; i++) {
            processFutures.get(i).complete(TRANSFORMER.apply(i));
        }

        cleanupSignal.acquire(half); // Wait until the processor finished internal cleanups.
        Futures.allOf(resultFutures.subList(0, half)).join();

        // Now add even more and make sure we are under capacity.
        for (int i = 0; i < CAPACITY / 2; i++) {
            val item = CAPACITY + i;
            resultFutures.add(p.execute(() -> itemProcessor.apply(item)));
            Assert.assertTrue("Item has not been immediately processed when under capacity: " + item, processedItems.contains(item));
        }

        // Finish up the remaining futures.
        for (int i = 0; i < processFutures.size(); i++) {
            val f = processFutures.get(i);
            if (!f.isDone()) {
                f.complete(TRANSFORMER.apply(i));
            }
        }

        // Verify they have been executed in order.
        val results = Futures.allOfWithResults(resultFutures).join();
        for (int i = 0; i < results.size(); i++) {
            Assert.assertEquals("Unexpected result at index " + i, TRANSFORMER.apply(i), results.get(i));
        }
    }

    /**
     * Tests a scenario where we add more items than the capacity allows. We want to verify that the items are queued up
     * and when their time comes, they get processed in order.
     */
    @Test
    public void testCapacityExceeded() {
        final int maxDelayMillis = 20;
        final int itemCount = 20 * CAPACITY;
        val processedItems = Collections.synchronizedCollection(new HashSet<Integer>());
        val processFuture = new CompletableFuture<Void>();

        // Each item wait for a signal to complete. When the signal arrives, each takes a random time to complete.
        val rnd = new Random(0);
        Supplier<Duration> delaySupplier = () -> Duration.ofMillis(rnd.nextInt(maxDelayMillis));
        Function<Integer, CompletableFuture<Integer>> itemProcessor = i -> {
            if (!processedItems.add(i)) {
                Assert.fail("Duplicate item detected: " + i);
            }

            CompletableFuture<Integer> result = new CompletableFuture<>();
            processFuture.thenComposeAsync(v -> Futures.delayedFuture(delaySupplier.get(), executorService()), executorService())
                         .whenCompleteAsync((r, ex) -> result.complete(TRANSFORMER.apply(i)));
            return result;
        };

        val resultFutures = new ArrayList<CompletableFuture<Integer>>();
        @Cleanup
        val p = new TestProcessor(CAPACITY, executorService());

        // Fill up to capacity, and beyond.
        for (int i = 0; i < itemCount; i++) {
            val item = i;
            resultFutures.add(p.execute(() -> itemProcessor.apply(item)));
            if (i >= CAPACITY) {
                Assert.assertFalse("Item has been immediately processed when over capacity: " + i, processedItems.contains(item));
            }
        }

        // Finish up the items, and verify new ones are being processed.
        processFuture.complete(null);

        // Verify they have been executed in order.
        val results = Futures.allOfWithResults(resultFutures).join();
        for (int i = 0; i < results.size(); i++) {
            Assert.assertEquals("Unexpected result at index " + i, TRANSFORMER.apply(i), results.get(i));
        }
    }

    @Test
    public void testFailures() throws Exception {
        final int itemCount = 2 * CAPACITY;
        val failedIndex = CAPACITY / 2;
        val processedItems = Collections.synchronizedCollection(new HashSet<Integer>());
        val failedFuture = new CompletableFuture<Integer>();
        Function<Integer, CompletableFuture<Integer>> itemProcessor = i -> {
            if (!processedItems.add(i)) {
                Assert.fail("Duplicate item detected: " + i);
            }

            if (i == failedIndex) {
                return failedFuture;
            }
            return CompletableFuture.completedFuture(i);
        };

        val resultFutures = new ArrayList<CompletableFuture<Integer>>();
        @Cleanup
        val p = new TestProcessor(CAPACITY, executorService());

        // Fill up to capacity, and beyond.
        for (int i = 0; i < itemCount; i++) {
            val item = i;
            resultFutures.add(p.execute(() -> itemProcessor.apply(item)));
        }

        // Fail an item.
        failedFuture.completeExceptionally(new IntentionalException());
        AssertExtensions.assertEventuallyEquals("Unexpected result size", itemCount, processedItems::size, 10, 10000);
        for (int i = 0; i < resultFutures.size(); i++) {
            val rf = resultFutures.get(i);
            if (i == failedIndex) {
                AssertExtensions.assertThrows(
                        "Failed item did not have its result failed as well.",
                        rf::join,
                        ex -> ex instanceof IntentionalException);
            } else {
                Assert.assertEquals("Unexpected completion.", i, (int) resultFutures.get(i).join());
            }
        }
    }

    /**
     * Tests that closing does cancel all pending items, except the processing ones.
     */
    @Test
    public void testClose() {
        final int itemCount = 2 * CAPACITY;
        val processedItems = Collections.synchronizedCollection(new HashSet<Integer>());
        val processFuture = new CompletableFuture<Integer>();
        Function<Integer, CompletableFuture<Integer>> itemProcessor = i -> {
            if (!processedItems.add(i)) {
                Assert.fail("Duplicate item detected: " + i);
            }

            return processFuture;
        };

        val resultFutures = new ArrayList<CompletableFuture<Integer>>();
        @Cleanup
        val p = new TestProcessor(CAPACITY, executorService());

        // Fill up to capacity, and beyond.
        for (int i = 0; i < itemCount; i++) {
            val item = i;
            resultFutures.add(p.execute(() -> itemProcessor.apply(item)));
        }

        p.close();

        // Verify all the items still pending have been cancelled, but not the ones currently executing.
        for (int i = 0; i < resultFutures.size(); i++) {
            val f = resultFutures.get(i);
            if (i < CAPACITY) {
                Assert.assertFalse(f.isDone());
            } else {
                Assert.assertTrue("Future not cancelled.", f.isCancelled());
            }
        }

        // Verify that the in-flight items can still be completed.
        processFuture.complete(10);

        for (int i = 0; i < CAPACITY; i++) {
            val f = resultFutures.get(i);
            Assert.assertEquals(10, (int) f.join());
        }
    }

    /**
     * Tests a scenario where all item processors finish immediately (they return a completed future).
     */
    @Test
    public void testInstantCompletion() {
        final int itemCount = 10000;
        Supplier<Integer> nextIndex = new AtomicInteger()::incrementAndGet;
        Function<Integer, CompletableFuture<Integer>> itemProcessor = i -> CompletableFuture.completedFuture(nextIndex.get());
        @Cleanup
        val p = new TestProcessor(CAPACITY, executorService());
        val resultFutures = new ArrayList<CompletableFuture<Integer>>(itemCount);
        for (int i = 0; i < itemCount; i++) {
            val item = i;
            resultFutures.add(p.execute(() -> itemProcessor.apply(item)));
        }

        // Verify they have been executed in order.
        val results = Futures.allOfWithResults(resultFutures).join();
        for (int i = 0; i < results.size(); i++) {
            Assert.assertEquals("Unexpected result at index " + i, i + 1, (int) results.get(i));
        }
    }

    private static class TestProcessor extends OrderedProcessor<Integer> {
        @Getter
        @Setter
        Runnable executionCompleteCallback;

        TestProcessor(int capacity, Executor executor) {
            super(capacity, executor);
        }

        @Override
        protected void executionComplete(Throwable exception) {
            super.executionComplete(exception);
            val callback = this.executionCompleteCallback;
            if (callback != null) {
                callback.run();
            }
        }
    }
}
