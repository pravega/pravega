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

import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
        val p = new SequentialAsyncProcessor(() -> {
            count.incrementAndGet();
            wasInvoked.release();
            waitOn.join();
        }, executorService());

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
}
