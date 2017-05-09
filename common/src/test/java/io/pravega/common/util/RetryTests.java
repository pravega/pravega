/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test methods for Retry utilities
 */
@Slf4j
public class RetryTests {

    final int maxLoops = 10;
    final int expectedResult = maxLoops * (maxLoops - 1) / 2;

    final long maxDelay = 100000;
    final long uniformDelay = 100;
    final long expectedDurationUniform = (maxLoops - 1) * uniformDelay;

    final long exponentialInitialDelay = 10;
    final int multiplier = 2;
    final long expectedDurationExponential = (long) (Math.pow(multiplier, maxLoops - 1) - 1) * exponentialInitialDelay;

    final AtomicInteger loopCounter = new AtomicInteger();
    final AtomicInteger accumulator = new AtomicInteger();

    Instant begin, end;
    long duration;

    private static class RetryableException extends RuntimeException {

    }

    private static class NonretryableException extends RuntimeException {

    }

    @Test
    public void retryTests() {

        // 1. series of retryable exceptions followed by a failure
        begin = Instant.now();
        int result = retry(uniformDelay, 1, maxLoops, maxDelay, true);
        end = Instant.now();
        duration = end.toEpochMilli() - begin.toEpochMilli();
        assertEquals(result, expectedResult);
        assertTrue(duration >= expectedDurationUniform);

        // 2. series of retryable exceptions followed by a non-retryable failure
        begin = Instant.now();
        try {
            retry(uniformDelay, 1, maxLoops, maxDelay, false);
        } catch (Exception e) {
            end = Instant.now();
            duration = end.toEpochMilli() - begin.toEpochMilli();
            assertTrue(duration >= expectedDurationUniform);
            assertTrue(e instanceof NonretryableException);
            assertEquals(accumulator.get(), expectedResult);
        }

        // 3. exponential backoff
        begin = Instant.now();
        result = retry(exponentialInitialDelay, multiplier, maxLoops, maxDelay, true);
        end = Instant.now();
        duration = end.toEpochMilli() - begin.toEpochMilli();
        assertEquals(result, expectedResult);
        assertTrue(duration >= expectedDurationExponential);

        // 4. exhaust retries
        begin = Instant.now();
        try {
            retry(uniformDelay, 1, maxLoops, maxDelay - 1, true);
        } catch (Exception e) {
            end = Instant.now();
            duration = end.toEpochMilli() - begin.toEpochMilli();
            assertTrue(duration >= expectedDurationUniform);
            assertTrue(e instanceof RetriesExhaustedException);
            assertTrue(e.getCause() instanceof RetryableException);
            assertEquals(accumulator.get(), expectedResult);
        }
    }

    @Test
    public void retryFutureTests() {
        ScheduledExecutorService executorService =
                Executors.newScheduledThreadPool(5, new ThreadFactoryBuilder().setNameFormat("testpool-%d").build());

        // 1. series of retryable exceptions followed by a failure
        begin = Instant.now();
        CompletableFuture<Integer> result = retryFuture(uniformDelay, 1, maxLoops, maxDelay, true, executorService);

        assertEquals(result.join().intValue(), expectedResult);
        end = Instant.now();
        duration = end.toEpochMilli() - begin.toEpochMilli();
        log.debug("Expected duration = {}", expectedDurationUniform);
        log.debug("Actual duration   = {}", duration);
        assertTrue(duration >= expectedDurationUniform);

        // 2, series of retryable exceptions followed by a non-retryable failure
        begin = Instant.now();
        result = retryFuture(uniformDelay, 1, maxLoops, maxDelay, false, executorService);
        try {
            result.join();
        } catch (CompletionException ce) {
            end = Instant.now();
            duration = end.toEpochMilli() - begin.toEpochMilli();
            log.debug("Expected duration = {}", expectedDurationUniform);
            log.debug("Actual duration   = {}", duration);
            assertTrue(duration >= expectedDurationUniform);
            assertTrue(ce.getCause() instanceof NonretryableException);
            assertEquals(accumulator.get(), expectedResult);
        }

        // 3. exponential backoff
        begin = Instant.now();
        result = retryFuture(exponentialInitialDelay, multiplier, maxLoops, maxDelay, true, executorService);
        assertEquals(result.join().intValue(), expectedResult);
        end = Instant.now();
        duration = end.toEpochMilli() - begin.toEpochMilli();
        log.debug("Expected duration = {}", expectedDurationExponential);
        log.debug("Actual duration   = {}", duration);
        assertTrue(duration >= expectedDurationExponential);

        // 4. Exhaust retries
        begin = Instant.now();
        result = retryFuture(uniformDelay, 1, maxLoops - 1, maxDelay, true, executorService);
        try {
            result.join();
        } catch (Exception e) {
            end = Instant.now();
            duration = end.toEpochMilli() - begin.toEpochMilli();
            log.debug("Expected duration = {}", expectedDurationUniform - uniformDelay);
            log.debug("Actual duration   = {}", duration);
            assertTrue(duration >= expectedDurationUniform - uniformDelay);
            assertTrue(e instanceof CompletionException);
            assertTrue(e.getCause() instanceof RetriesExhaustedException);
            assertTrue(e.getCause().getCause() instanceof CompletionException);
            assertTrue(e.getCause().getCause().getCause() instanceof RetryableException);
        }
    }

    @Test
    public void retryPredicateTest() {
        AtomicInteger i = new AtomicInteger(0);
        try {
            Retry.withExpBackoff(10, 10, 10)
                    .retryWhen(e -> i.getAndIncrement() != 1)
                    .throwingOn(RuntimeException.class)
                    .run(() -> {
                        throw new Exception("test");
                    });
        } catch (Exception e) {
            assert i.get() == 2;
        }
    }

    @Test
    public void retryIndefiniteTest() throws ExecutionException, InterruptedException {
        AtomicInteger i = new AtomicInteger(0);
        Retry.indefinitelyWithExpBackoff(10, 10, 10, e -> i.getAndIncrement())
                .runAsync(() -> CompletableFuture.runAsync(() -> {
                    if (i.get() < 10) {
                        throw new RuntimeException("test");
                    }
                }), Executors.newSingleThreadScheduledExecutor()).get();
        assert i.get() == 10;
    }


    private int retry(long delay,
                      int multiplier,
                      int attempts,
                      long maxDelay,
                      boolean success) {

        loopCounter.set(0);
        accumulator.set(0);
        return Retry.withExpBackoff(delay, multiplier, attempts, maxDelay)
                .retryingOn(RetryableException.class)
                .throwingOn(NonretryableException.class)
                .run(() -> {
                    accumulator.getAndAdd(loopCounter.getAndIncrement());
                    int i = loopCounter.get();
                    log.debug("Loop counter = " + i);
                    if (i % 10 == 0) {
                        if (success) {
                            return accumulator.get();
                        } else {
                            throw new NonretryableException();
                        }
                    } else {
                        throw new RetryableException();
                    }
                });
    }

    private CompletableFuture<Integer> retryFuture(final long delay,
                                                   final int multiplier,
                                                   final int attempts,
                                                   final long maxDelay,
                                                   final boolean success,
                                                   final ScheduledExecutorService executorService) {

        loopCounter.set(0);
        accumulator.set(0);
        return Retry.withExpBackoff(delay, multiplier, attempts, maxDelay)
                .retryingOn(RetryableException.class)
                .throwingOn(NonretryableException.class)
                .runAsync(() -> futureComputation(success, executorService), executorService);
    }

    private CompletableFuture<Integer> futureComputation(boolean success, ScheduledExecutorService executorService) {
        return CompletableFuture.supplyAsync(() -> {
            accumulator.getAndAdd(loopCounter.getAndIncrement());
            int i = loopCounter.get();
            log.debug("Loop counter = {}, timestamp={}", i, Instant.now());
            if (i % 10 == 0) {
                if (success) {
                    return accumulator.get();
                } else {
                    throw new NonretryableException();
                }
            } else {
                throw new RetryableException();
            }
        }, executorService);
    }
}
