/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.common.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test methods for Retry utilities
 */
public class RetryTests {

    private static class RetryableException extends RuntimeException {

    }

    private static class NonretryableException extends RuntimeException {

    }

    @Test
    public void retryTests() {
        final int maxLoops = 10;
        final int expectedResult = maxLoops * (maxLoops - 1) / 2;
        AtomicInteger loopCounter = new AtomicInteger();
        AtomicInteger accumulator = new AtomicInteger();

        // 1. series of retryable exceptions followed by a failure
        int result =
        Retry.withExpBackoff(100, 1, maxLoops, 100000)
                .retryingOn(RetryableException.class)
                .throwingOn(NonretryableException.class)
                .run(() -> {
                    accumulator.getAndAdd(loopCounter.getAndIncrement());
                    int i = loopCounter.get();
                    if (i % 10 == 0) {
                        return accumulator.get();
                    } else {
                        throw new RetryableException();
                    }
                });

        Assert.assertEquals(result, expectedResult);

        // 2, series of retryable exceptions followed by a non-retryable failure
        loopCounter.set(0);
        accumulator.set(0);
        try {
            Retry.withExpBackoff(100, 1, maxLoops, 100000)
                    .retryingOn(RetryableException.class)
                    .throwingOn(NonretryableException.class)
                    .run(() -> {
                        accumulator.getAndAdd(loopCounter.getAndIncrement());
                        int i = loopCounter.get();
                        if (i % 10 == 0) {
                            throw new NonretryableException();
                        } else {
                            throw new RetryableException();
                        }
                    });
        } catch (Exception e) {
            Assert.assertTrue(e instanceof NonretryableException);
            Assert.assertEquals(accumulator.get(), expectedResult);
        }
    }

    @Test
    public void retryFutureTests() {
        final int maxLoops = 10;
        final int expectedResult = maxLoops * (maxLoops - 1) / 2;
        AtomicInteger loopCounter = new AtomicInteger();
        AtomicInteger accumulator = new AtomicInteger();
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(100);

        // 1. series of retryable exceptions followed by a failure
        CompletableFuture<Integer> result =
                Retry.withExpBackoff(100, 1, maxLoops, 100000)
                        .retryingOn(RetryableException.class)
                        .throwingOn(NonretryableException.class)
                        .runFuture(() ->
                                        CompletableFuture.supplyAsync(() -> {
                                            accumulator.getAndAdd(loopCounter.getAndIncrement());
                                            int i = loopCounter.get();
                                            if (i % 10 == 0) {
                                                return accumulator.get();
                                            } else {
                                                throw new RetryableException();
                                            }
                                        }), executorService);

        Assert.assertEquals(result.join().intValue(), expectedResult);

        // 2, series of retryable exceptions followed by a non-retryable failure
        loopCounter.set(0);
        accumulator.set(0);
        result =
                Retry.withExpBackoff(100, 1, maxLoops, 100000)
                        .retryingOn(RetryableException.class)
                        .throwingOn(NonretryableException.class)
                        .runFuture(() ->
                                CompletableFuture.supplyAsync(() -> {
                                    accumulator.getAndAdd(loopCounter.getAndIncrement());
                                    int i = loopCounter.get();
                                    if (i % 10 == 0) {
                                        throw new NonretryableException();
                                    } else {
                                        throw new RetryableException();
                                    }
                                }), executorService);
        try {
            result.join();
        } catch (CompletionException ce) {
            Assert.assertTrue(ce.getCause() instanceof NonretryableException);
            Assert.assertEquals(accumulator.get(), expectedResult);
        }
    }
}
