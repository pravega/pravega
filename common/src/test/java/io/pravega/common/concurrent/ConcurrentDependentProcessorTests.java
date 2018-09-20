/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.concurrent;

import io.pravega.common.ObjectClosedException;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the ConcurrentDependentProcessor class.
 */
public class ConcurrentDependentProcessorTests extends ThreadPooledTestSuite {
    private static final int SHORT_TIMEOUT_MILLIS = 50;
    private static final int TIMEOUT_MILLIS = 10000;

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    /**
     * Tests the processor using two keys and verifies the tasks are executed in parallel.
     */
    @Test
    public void testParallelism() throws Exception {
        final int key1 = 1;
        final int key2 = 2;
        @Cleanup
        val proc = new ConcurrentDependentProcessor<Integer, Integer>(executorService());
        val toRun1 = new CompletableFuture<Integer>();
        val result1 = proc.add(Collections.singleton(key1), () -> toRun1);

        val toRun2 = new CompletableFuture<Integer>();
        val result2 = proc.add(Collections.singleton(key2), () -> toRun2);
        Assert.assertFalse("Not expecting anything to be done yet.", result1.isDone() || result2.isDone());

        // Complete second run. If it completes, we have verified that it hasn't been blocked on the first one.
        toRun2.complete(10);
        result2.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected result from second key run.", result2.join(), toRun2.join());
        Assert.assertFalse("Not expecting first task to be done yet.", result1.isDone());

        // Complete the first run.
        toRun1.complete(20);
        result1.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected result from first key run.", result1.join(), toRun1.join());
    }

    /**
     * Tests the processor using a single dependency key for each task. This means that tasks with different keys
     * can execute in parallel while tasks with the same key may not.
     */
    @Test
    public void testSingleDependencyKey() throws Exception {
        final int key = 1;
        final int count = 10000;
        @Cleanup
        val proc = new ConcurrentDependentProcessor<Integer, Integer>(executorService());
        val running = new AtomicBoolean(false);
        val previousRun = new AtomicReference<CompletableFuture<Integer>>();
        val results = new ArrayList<CompletableFuture<Integer>>();
        for (int i = 0; i < count; i++) {
            val thisRun = new CompletableFuture<Integer>();
            val pr = previousRun.getAndSet(thisRun);
            results.add(proc.add(Collections.singleton(key), () -> {
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
     * Tests the processor using multiple dependency keys for each task. This means that tasks with disjoint dependency
     * keys may execute in parallel, while the others may not.
     */
    @Test
    public void testMultiDependencyKey() throws Exception {
        final int key1 = 1;
        final int key2 = 2;
        final int key3 = 3;
        @Cleanup
        val proc = new ConcurrentDependentProcessor<Integer, Integer>(executorService());

        // We setup two individual tasks to begin with.
        val toRun1 = new CompletableFuture<Integer>();
        val result1 = proc.add(Collections.singleton(key1), () -> toRun1);

        val toRun2 = new CompletableFuture<Integer>();
        val result2 = proc.add(Collections.singleton(key2), () -> toRun2);
        Assert.assertFalse("Not expecting anything to be done yet.", result1.isDone() || result2.isDone());

        // We setup the third task, which depends on both of the original tasks.
        val result3 = proc.add(Arrays.asList(key1, key2, key3), () -> {
            Assert.assertTrue("Not expecting third task to execute yet.", result1.isDone() && result2.isDone());
            return CompletableFuture.completedFuture(3);
        });

        // Task 4 depends on Key1, which was last used during Task 3.
        val result4 = proc.add(Collections.singleton(key1), () -> {
            Assert.assertTrue("Not expecting fourth task to execute yet.", result3.isDone());
            return CompletableFuture.completedFuture(4);
        });

        // Task 5 depends on Key3, which was only introduced as a group with Task 3.
        val result5 = proc.add(Collections.singleton(key3), () -> {
            Assert.assertTrue("Not expecting fifth task to execute yet.", result3.isDone());
            return CompletableFuture.completedFuture(5);
        });

        // Complete the first task. Verify it did complete, but it didn't unblock the third one.
        toRun1.complete(1);
        result1.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected result from first key run.", result1.join(), toRun1.join());

        AssertExtensions.assertThrows(
                "Third task unexpectedly completed.",
                () -> result3.get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                ex -> ex instanceof TimeoutException);

        // Complete the second task. Verify both it and the third task completed.
        toRun2.complete(2);
        result2.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected result from second key run.", result2.join(), toRun2.join());

        result3.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected result from third key run.", (int) result3.join(), 3);

        result4.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected result from task 4.", (int) result4.join(), 4);

        result5.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected result from task 5.", (int) result5.join(), 5);
    }

    /**
     * Tests the ability to cancel ongoing tasks when the processor is closed.
     */
    @Test
    public void testClose() {
        final int key = 1;
        @Cleanup
        val proc = new ConcurrentDependentProcessor<Integer, Integer>(executorService());
        val toRun = new CompletableFuture<Integer>();
        val result = proc.add(Collections.singleton(key), () -> toRun);

        proc.close();
        AssertExtensions.assertThrows(
                "Task not cancelled.",
                result::join,
                ex -> ex instanceof ObjectClosedException);

        Assert.assertFalse("Not expecting inner blocker task to be done.", toRun.isDone());
    }
}
