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
package io.pravega.segmentstore.storage.impl.bookkeeper;

import io.pravega.common.Exceptions;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.common.util.Retry;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the SequentialAsyncProcessor class.
 */
public class SequentialAsyncProcessorTests extends ThreadPooledTestSuite {
    private static final int TIMEOUT_MILLIS = 10000;

    @Rule
    public Timeout globalTimeout = new Timeout(10, TimeUnit.SECONDS);

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the runAsync() method.
     */
    @Test(timeout = TIMEOUT_MILLIS)
    public void testRunAsync() throws Exception {
        final int invocationCount = 10;
        val count = new AtomicInteger();
        val wasInvoked = new Semaphore(0);
        val waitOn = new CompletableFuture<Void>();
        val retry = Retry.withExpBackoff(1, 2, 3)
                         .retryWhen(t -> true);
        val error = new AtomicReference<Throwable>();
        @Cleanup
        val p = new SequentialAsyncProcessor(
                () -> {
                    count.incrementAndGet();
                    wasInvoked.release();
                    waitOn.join();
                },
                retry,
                error::set,
                executorService());

        // Invoke it a number of times.
        for (int i = 0; i < invocationCount; i++) {
            p.runAsync();
        }

        // Wait for at least one invocation to happen.
        wasInvoked.acquire();
        Assert.assertEquals("Task seems to have been executed concurrently.", 1, count.get());

        // Now complete the first task and ensure the subsequent requests only result in on one extra invocations.
        waitOn.complete(null);
        wasInvoked.acquire();
        Assert.assertEquals("Unexpected number of final invocations.", 2, count.get());
    }

    /**
     * Tests the runAsync() method with execution errors.
     */
    @Test(timeout = TIMEOUT_MILLIS)
    public void testRunAsyncErrors() throws Exception {
        final int expectedCount = 2;
        val count = new AtomicInteger();
        val finished = new CompletableFuture<Void>();
        val retry = Retry.withExpBackoff(1, 2, expectedCount)
                         .retryWhen(t -> {
                             if (count.get() >= expectedCount) {
                                 finished.complete(null);
                             }
                             return Exceptions.unwrap(t) instanceof IntentionalException;
                         });
        val error = new CompletableFuture<Throwable>();
        @Cleanup
        val p = new SequentialAsyncProcessor(
                () -> {
                    count.incrementAndGet();
                    throw new IntentionalException();
                },
                retry,
                error::complete,
                executorService());

        // Invoke it once.
        p.runAsync();

        finished.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        val finalException = Exceptions.unwrap(error.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
        Assert.assertEquals("Unexpected number of final invocations.", expectedCount, count.get());
        Assert.assertTrue("Unexpected final error callback.", finalException instanceof RetriesExhaustedException
                && Exceptions.unwrap(finalException.getCause()) instanceof IntentionalException);
    }
}
