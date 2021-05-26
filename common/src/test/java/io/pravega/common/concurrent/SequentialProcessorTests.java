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

import io.pravega.common.ObjectClosedException;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the {@link SequentialProcessor} class.
 */
public class SequentialProcessorTests extends ThreadPooledTestSuite {
    private static final int TIMEOUT_MILLIS = 10000;

    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    /**
     * Tests the {@link SequentialProcessor#add(Supplier)} method.
     */
    @Test
    public void testAdd() throws Exception {
        final int count = 10000;
        @Cleanup
        val proc = new SequentialProcessor(executorService());
        val running = new AtomicBoolean(false);
        val previousRun = new AtomicReference<CompletableFuture<Integer>>();
        val results = new ArrayList<CompletableFuture<Integer>>();
        for (int i = 0; i < count; i++) {
            val thisRun = new CompletableFuture<Integer>();
            val pr = previousRun.getAndSet(thisRun);
            results.add(proc.add(() -> {
                if (!running.compareAndSet(false, true)) {
                    Assert.fail("Concurrent execution detected.");
                }

                return thisRun.thenApply(r -> {
                    running.set(false);
                    return r;
                });
            }));

            if (pr != null) {
                pr.complete(i - 1);
            }
        }

        // Complete the last one.
        previousRun.get().complete(count - 1);

        for (int i = 0; i < results.size(); i++) {
            val value = results.get(i).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            Assert.assertEquals("Unexpected value.", i, (int) value);
        }
    }

    /**
     * Tests the ability to cancel ongoing tasks when the processor is closed.
     */
    @Test
    public void testClose() {
        @Cleanup
        val proc = new SequentialProcessor(executorService());
        val toRun = new CompletableFuture<Integer>();
        val result = proc.add(() -> toRun);

        proc.close();
        AssertExtensions.assertThrows(
                "Task not cancelled.",
                result::join,
                ex -> ex instanceof ObjectClosedException);

        Assert.assertFalse("Not expecting inner blocker task to be done.", toRun.isDone());
    }
}
