/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.util;

import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.RetriesExhaustedException;
import io.pravega.controller.retryable.RetryableException;
import io.pravega.test.common.ThreadPooledTestSuite;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertTrue;

public class RetryHelperTest extends ThreadPooledTestSuite {
    private static class TestException extends RuntimeException implements RetryableException {
    }

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Test(timeout = 10000)
    public void testWithRetries() {
        try {
            RetryHelper.withRetries(() -> {
                throw new TestException();
            }, RetryHelper.RETRYABLE_PREDICATE, 2);
        } catch (Exception e) {
            assertTrue(e instanceof RetriesExhaustedException);
            Throwable ex = Exceptions.unwrap(e.getCause());
            assertTrue(ex instanceof TestException);
        }

        AtomicInteger count = new AtomicInteger(0);
        assertTrue(RetryHelper.withRetries(() -> {
            if (count.incrementAndGet() < 2) {
                throw new TestException();
            }
            return count.get();
        }, RetryHelper.RETRYABLE_PREDICATE, 2) == 2);

        assertTrue(RetryHelper.withRetries(() -> {
            if (count.incrementAndGet() < 4) {
                throw new RuntimeException();
            }
            return count.get();
        }, RetryHelper.UNCONDITIONAL_PREDICATE, 2) == 4);
    }

    @Test(timeout = 10000)
    public void testWithRetriesAsync() {

        try {
            RetryHelper.withRetriesAsync(() -> CompletableFuture.runAsync(() -> {
                throw new TestException();
            }), RetryHelper.RETRYABLE_PREDICATE, 2, executorService());
        } catch (Exception e) {
            assertTrue(e instanceof RetriesExhaustedException);
            Throwable ex = Exceptions.unwrap(e.getCause());
            assertTrue(ex instanceof TestException);
        }

        AtomicInteger count = new AtomicInteger(0);
        assertTrue(Futures.getAndHandleExceptions(RetryHelper.withRetriesAsync(() -> CompletableFuture.supplyAsync(() -> {
            if (count.incrementAndGet() < 2) {
                throw new TestException();
            }
            return count.get();
        }), RetryHelper.RETRYABLE_PREDICATE, 2, executorService()), RuntimeException::new) == 2);

        assertTrue(Futures.getAndHandleExceptions(RetryHelper.withRetriesAsync(() -> CompletableFuture.supplyAsync(() -> {
            if (count.incrementAndGet() < 4) {
                throw new RuntimeException();
            }
            return count.get();
        }), RetryHelper.UNCONDITIONAL_PREDICATE, 2, executorService()), RuntimeException::new) == 4);
    }

    @Test(timeout = 30000L)
    public void testLoopWithDelay() {
        final int maxLoops = 5;
        AtomicInteger loopCounter = new AtomicInteger();
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

        AtomicLong previous = new AtomicLong(System.nanoTime());
        AtomicBoolean loopDelayHonored = new AtomicBoolean(true);

        long oneSecondInNano = Duration.ofSeconds(1).toNanos();
        long delayInMs = Duration.ofSeconds(1).toMillis();
        // try multiple loops and verify that across multiple loops the delay is honoured
        RetryHelper.loopWithDelay(
                () -> loopCounter.incrementAndGet() < maxLoops,
                () -> {
                    loopDelayHonored.compareAndSet(true, System.nanoTime() - previous.get() >= oneSecondInNano);
                    previous.set(System.nanoTime());

                    return CompletableFuture.completedFuture(null);
                },
                delayInMs,
                executorService
        ).join();
        Assert.assertTrue(loopDelayHonored.get());
    }
}
