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
import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.IntentionalException;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the ExecutorServiceHelpers class.
 * NOTE: this class inherits from ThreadPooledTestSuite but does not set a custom ThreadPoolSize. The Default (0) indicates
 * we are using an InlineThreadPool, which is what all these tests rely on.
 */
public class ExecutorServiceHelpersTests extends ThreadPooledTestSuite {

    /**
     * Tests the execute() method.
     */
    @Test(timeout = 5000)
    public void testExecute() {
        AtomicInteger runCount = new AtomicInteger();
        AtomicReference<Throwable> exceptionHolder = new AtomicReference<>();
        AtomicInteger finallyCount = new AtomicInteger();

        // Normal execution
        ExecutorServiceHelpers.execute(
                runCount::incrementAndGet,
                exceptionHolder::set,
                finallyCount::incrementAndGet,
                executorService());

        Assert.assertEquals("Unexpected number of runs (normal execution)", 1, runCount.get());
        Assert.assertNull("Unexpected exception set (normal execution)", exceptionHolder.get());
        Assert.assertEquals("Unexpected number of finally runs (normal execution)", 1, finallyCount.get());

        // Run with failure.
        runCount.set(0);
        exceptionHolder.set(null);
        finallyCount.set(0);
        ExecutorServiceHelpers.execute(
                () -> {
                    throw new IntentionalException();
                },
                exceptionHolder::set,
                finallyCount::incrementAndGet,
                executorService());
        Assert.assertTrue("Unexpected exception set (failed task)", exceptionHolder.get() instanceof IntentionalException);
        Assert.assertEquals("Unexpected number of finally runs (failed task)", 1, finallyCount.get());

        // Scheduling exception
        val closedExecutor = Executors.newSingleThreadExecutor();
        ExecutorServiceHelpers.shutdown(closedExecutor);
        runCount.set(0);
        exceptionHolder.set(null);
        finallyCount.set(0);
        AssertExtensions.assertThrows(
                "execute did not throw appropriate exception when executor was closed",
                () -> ExecutorServiceHelpers.execute(
                        runCount::incrementAndGet,
                        exceptionHolder::set,
                        finallyCount::incrementAndGet,
                        closedExecutor),
                ex -> ex instanceof RejectedExecutionException);
        Assert.assertEquals("Unexpected number of runs (rejected execution)", 0, runCount.get());
        Assert.assertNull("Unexpected exception set (rejected execution)", exceptionHolder.get());
        Assert.assertEquals("Unexpected number of finally runs (rejected execution)", 1, finallyCount.get());
    }

    @Test
    public void testSnapshot() {
        @Cleanup("shutdown")
        ScheduledExecutorService coreExecutor = ExecutorServiceHelpers.newScheduledThreadPool(30, "core", Thread.NORM_PRIORITY);

        ExecutorServiceHelpers.Snapshot snapshot = ExecutorServiceHelpers.getSnapshot(coreExecutor);
        Assert.assertEquals("Unexpected pool size", 30, snapshot.getPoolSize());
        Assert.assertEquals("Unexpected queue size", 0, snapshot.getQueueSize());

        ScheduledExecutorService inlineExecutor = new InlineExecutor();
        ExecutorServiceHelpers.Snapshot inlineSnapshot = ExecutorServiceHelpers.getSnapshot(inlineExecutor);
        Assert.assertNull("Unexpected snapshot", inlineSnapshot);

        ExecutorServiceHelpers.shutdown(Duration.ofSeconds(1), coreExecutor, inlineExecutor);
    }
}
