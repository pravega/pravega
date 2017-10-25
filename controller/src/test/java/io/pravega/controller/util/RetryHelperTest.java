/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

public class RetryHelperTest {
    private static class TestException extends RuntimeException implements RetryableException {
    }

    ScheduledExecutorService executor;

    @Before
    public void setup() {
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void teardown() {
        executor.shutdown();
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
            }), RetryHelper.RETRYABLE_PREDICATE, 2, executor);
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
        }), RetryHelper.RETRYABLE_PREDICATE, 2, executor), RuntimeException::new) == 2);

        assertTrue(Futures.getAndHandleExceptions(RetryHelper.withRetriesAsync(() -> CompletableFuture.supplyAsync(() -> {
            if (count.incrementAndGet() < 4) {
                throw new RuntimeException();
            }
            return count.get();
        }), RetryHelper.UNCONDITIONAL_PREDICATE, 2, executor), RuntimeException::new) == 4);
    }
}
