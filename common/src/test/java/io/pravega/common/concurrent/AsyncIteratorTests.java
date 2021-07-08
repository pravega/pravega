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

import com.google.common.base.Preconditions;
import io.pravega.common.util.AsyncIterator;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.concurrent.GuardedBy;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the default methods on the {@link AsyncIterator} interface.
 */
public class AsyncIteratorTests extends ThreadPooledTestSuite {
    private static final int TIMEOUT_MILLIS = 30000;
    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Test
    public void testSingleton() {
        val item = 1;
        val iterator = AsyncIterator.singleton(item);
        val retrievedItem = iterator.getNext().join();
        Assert.assertEquals(item, (int) retrievedItem);
        Assert.assertNull(iterator.getNext().join());
    }

    /**
     * Tests the {@link AsyncIterator#forEachRemaining(Consumer, Executor)} method.
     */
    @Test
    public void testForEachRemaining() {
        val expected = IntStream.range(0, 10).boxed().collect(Collectors.toList());
        val iterator = new TestIterator<Integer>(expected.stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));
        val result = new ArrayList<Integer>();
        iterator.forEachRemaining(result::add, executorService()).join();
        AssertExtensions.assertListEquals("Unexpected result.", expected, result, Integer::equals);
    }

    /**
     * Tests the {@link AsyncIterator#collectRemaining(Predicate)} method.
     */
    @Test
    public void testCollectRemaining() {
        val expected = IntStream.range(0, 10).boxed().collect(Collectors.toList());

        val iterator1 = new TestIterator<Integer>(expected.stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));
        val result1 = new ArrayList<Integer>();
        iterator1.collectRemaining(result1::add).join();
        AssertExtensions.assertListEquals("Unexpected result.", expected, result1, Integer::equals);

        val iterator2 = new TestIterator<Integer>(expected.stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));
        val result2 = new ArrayList<Integer>();
        iterator2.collectRemaining(e -> {
            if (e < 5) {
                result2.add(e);
                return true;
            } else {
                return false;
            }
        });
        AssertExtensions.assertListEquals("Unexpected result.", IntStream.range(0, 5).boxed().collect(Collectors.toList()),
                                          result2, Integer::equals);
    }

    /**
     * Tests the {@link AsyncIterator#asSequential(Executor)} method.
     */
    @Test
    public void testAsSequential() throws Exception {
        int count = 100;
        val expectedItems = new ArrayList<CompletableFuture<Integer>>();
        for (int i = 0; i < count; i++) {
            expectedItems.add(new CompletableFuture<>());
        }

        val iterator = new TestIterator<>(expectedItems).asSequential(executorService());
        val iteratorItems = new ArrayList<CompletableFuture<Integer>>();
        for (int i = 0; i <= expectedItems.size(); i++) {
            iteratorItems.add(iterator.getNext());
        }

        for (int i = 0; i < expectedItems.size(); i++) {
            val current = expectedItems.get(i);
            val fromIterator = iteratorItems.get(i);
            if (i % 2 == 0) {
                current.complete(i);
                val actualValue = fromIterator.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
                Assert.assertEquals("Unexpected result for non-exceptional iteration.", i, (int) actualValue);
            } else {
                current.completeExceptionally(new IntentionalException(Integer.toString(i)));
                AssertExtensions.assertSuppliedFutureThrows(
                        "Unexpected behavior for exceptional iteration.",
                        () -> fromIterator,
                        ex -> ex instanceof IntentionalException);
            }

            // Verify no subsequent future was completed. An exception to this rule is the very last one, which will complete
            // with a null value when the one right before it is done.
            for (int j = iteratorItems.size() - 2; j > i; j--) {
                Assert.assertFalse("Not expecting subsequent calls to be completed (i=" + i + ", j=" + j + ").", iteratorItems.get(j).isDone());
            }
        }

        // Last case; we purposefully made an extra invocation of #getNext() to observe its behavior.
        val last = iteratorItems.get(iteratorItems.size() - 1).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertNull("Expected last value to be null", last);
    }

    /**
     * Tests the {@link AsyncIterator#thenApply} method.
     */
    @Test
    public void testThenApply() {
        val expected = IntStream.range(0, 10).boxed().collect(Collectors.toList());
        val baseIterator = new TestIterator<Integer>(expected.stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));
        val newIterator = baseIterator.thenApply(Object::toString);
        val result = new ArrayList<String>();
        newIterator.forEachRemaining(result::add, executorService()).join();
        val expectedResult = expected.stream().map(Object::toString).collect(Collectors.toList());
        AssertExtensions.assertListEquals("Unexpected result.", expectedResult, result, String::equals);
    }

    /**
     * Tests the {@link AsyncIterator#thenCompose} method.
     */
    @Test
    public void testThenCompose() {
        val expected = IntStream.range(0, 10).boxed().collect(Collectors.toList());
        val baseIterator = new TestIterator<Integer>(expected.stream().map(CompletableFuture::completedFuture).collect(Collectors.toList()));
        AsyncIterator<String> newIterator = baseIterator.thenCompose(i -> CompletableFuture.completedFuture(i.toString()));
        val result = new ArrayList<String>();
        newIterator.forEachRemaining(result::add, executorService()).join();
        val expectedResult = expected.stream().map(Object::toString).collect(Collectors.toList());
        AssertExtensions.assertListEquals("Unexpected result.", expectedResult, result, String::equals);
    }

    private static class TestIterator<T> implements AsyncIterator<T> {
        @GuardedBy("items")
        private final Iterator<CompletableFuture<T>> items;
        private final AtomicBoolean inProgress;

        TestIterator(List<CompletableFuture<T>> items) {
            this.items = items.iterator();
            this.inProgress = new AtomicBoolean();
        }

        @Override
        public CompletableFuture<T> getNext() {
            Preconditions.checkState(this.inProgress.compareAndSet(false, true), "Concurrent call to getNext().");
            try {
                synchronized (this.items) {
                    return this.items.hasNext() ? this.items.next() : CompletableFuture.completedFuture(null);
                }
            } finally {
                this.inProgress.set(false);
            }
        }
    }
}
