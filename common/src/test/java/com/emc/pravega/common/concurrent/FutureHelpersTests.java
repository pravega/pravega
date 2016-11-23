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
package com.emc.pravega.common.concurrent;

import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.IntentionalException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for the FutureHelpers class.
 */
public class FutureHelpersTests {
    /**
     * Tests the failedFuture() method.
     */
    @Test
    public void testFailedFuture() {
        Throwable ex = new IntentionalException();
        CompletableFuture<Void> cf = FutureHelpers.failedFuture(ex);
        Assert.assertTrue("failedFuture() did not create a failed future.", cf.isCompletedExceptionally());
        AssertExtensions.assertThrows("failedFuture() did not complete the future with the expected exception.",
                cf::join, e -> e.equals(ex));
    }

    /**
     * Tests the exceptionListener() method.
     */
    @Test
    public void testExceptionListener() {
        AtomicReference<Throwable> thrownException = new AtomicReference<>();
        CompletableFuture<Void> cf = new CompletableFuture<>();
        FutureHelpers.exceptionListener(cf, thrownException::set);
        cf.complete(null);
        Assert.assertNull("exceptionListener invoked the callback when the future was completed normally.",
                thrownException.get());

        thrownException.set(null);
        cf = new CompletableFuture<>();
        Exception ex = new IntentionalException();
        FutureHelpers.exceptionListener(cf, thrownException::set);
        cf.completeExceptionally(ex);
        Assert.assertNotNull("exceptionListener did not invoke the callback when the future was completed " +
                        "exceptionally.",
                thrownException.get());
        Assert.assertEquals("Unexpected exception was passed to the callback from exceptionListener when the future " +
                        "was completed exceptionally.", ex, thrownException.get());
    }

    /**
     * Tests the allOf() method.
     */
    @Test
    public void testAllOf() {
        int count = 10;

        // Already completed futures.
        List<CompletableFuture<Integer>> futures = createNumericFutures(count);
        completeFutures(futures);
        CompletableFuture<Void> allFuturesComplete = FutureHelpers.allOf(futures);
        Assert.assertTrue("allOf() did not create a completed future when all futures were previously complete.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());

        // Not completed futures.
        futures = createNumericFutures(count);
        allFuturesComplete = FutureHelpers.allOf(futures);
        Assert.assertFalse("allOf() created a completed future when none of the futures were previously complete.",
                allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOf() complete when all its futures completed.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());

        // At least one failed & completed future.
        futures = createNumericFutures(count);
        failRandomFuture(futures);
        allFuturesComplete = FutureHelpers.allOf(futures);
        Assert.assertFalse("allOf() created a completed future when not all of the futures were previously complete " +
                        "(but one failed).", allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOf() did not complete exceptionally when at least one of the futures " +
                "failed.", allFuturesComplete.isCompletedExceptionally());

        // At least one failed future.
        futures = createNumericFutures(count);
        allFuturesComplete = FutureHelpers.allOf(futures);
        failRandomFuture(futures);
        Assert.assertFalse("The result of allOf() completed when not all the futures completed (except one that " +
                        "failed).", allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOf() did not complete exceptionally when at least one of the futures " +
                        "failed.", allFuturesComplete.isCompletedExceptionally());
    }

    /**
     * Tests the allOfWithResults() method.
     */
    @Test
    public void testAllOfWithResults() {
        int count = 10;

        // Already completed futures.
        List<CompletableFuture<Integer>> futures = createNumericFutures(count);
        completeFutures(futures);
        CompletableFuture<Collection<Integer>> allFuturesComplete = FutureHelpers.allOfWithResults(futures);
        Assert.assertTrue("allOf() did not create a completed future when all futures were previously complete.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkResults(allFuturesComplete.join());

        // Not completed futures.
        futures = createNumericFutures(count);
        allFuturesComplete = FutureHelpers.allOfWithResults(futures);
        Assert.assertFalse("allOf() created a completed future when none of the futures were previously complete.",
                allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOf() complete when all its futures completed.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkResults(allFuturesComplete.join());

        // At least one failed & completed future.
        futures = createNumericFutures(count);
        failRandomFuture(futures);
        allFuturesComplete = FutureHelpers.allOfWithResults(futures);
        Assert.assertFalse("allOf() created a completed future when not all of the futures were previously complete " +
                        "(but one failed).", allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue( "The result of allOf() did not complete exceptionally when at least one of the futures " +
                        "failed.", allFuturesComplete.isCompletedExceptionally());

        // At least one failed future.
        futures = createNumericFutures(count);
        allFuturesComplete = FutureHelpers.allOfWithResults(futures);
        failRandomFuture(futures);
        Assert.assertFalse("The result of allOf() completed when not all the futures completed (except one that " +
                        "failed).", allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue( "The result of allOf() did not complete exceptionally when at least one of the futures " +
                        "failed.", allFuturesComplete.isCompletedExceptionally());
    }

    @Test
    public void testWhileLoop() {
        final int maxLoops = 10;
        final int expectedResult = maxLoops * (maxLoops - 1) / 2;
        AtomicInteger loopCounter = new AtomicInteger();
        AtomicInteger accumulator = new AtomicInteger();

        // 1. With no specific accumulator.
        FutureHelpers.loop(
                () -> loopCounter.incrementAndGet() < maxLoops,
                () -> {
                    accumulator.addAndGet(loopCounter.get());
                    return CompletableFuture.completedFuture(null);
                },
                ForkJoinPool.commonPool()).join();
        Assert.assertEquals("Unexpected result for loop without a specific accumulator.", expectedResult,
                accumulator.get());

        //2. With specific accumulator.
        loopCounter.set(0);
        accumulator.set(0);
        FutureHelpers.loop(
                () -> loopCounter.incrementAndGet() < maxLoops,
                () -> CompletableFuture.completedFuture(loopCounter.get()),
                accumulator::addAndGet,
                ForkJoinPool.commonPool()).join();
        Assert.assertEquals("Unexpected result for loop with a specific accumulator.", expectedResult,
                accumulator.get());

        //3. With exceptions.
        loopCounter.set(0);
        accumulator.set(0);
        CompletableFuture<Void> loopFuture = FutureHelpers.loop(
                () -> loopCounter.incrementAndGet() < maxLoops,
                () -> {
                    if (loopCounter.get() % 3 == 0) {
                        throw new IntentionalException();
                    } else {
                        accumulator.addAndGet(loopCounter.get());
                        return CompletableFuture.completedFuture(null);
                    }
                },
                ForkJoinPool.commonPool());

        AssertExtensions.assertThrows(
                "loop() did not return a failed Future when one of the loopBody calls returned a failed Future.",
                loopFuture::join,
                ex -> ex instanceof IntentionalException);
        Assert.assertEquals("Unexpected value accumulated until loop was interrupted.", 3, accumulator.get());
    }

    @Test
    public void testDoWhileLoopWithCondition() {
        final int maxLoops = 10;
        final int expectedResult = maxLoops * (maxLoops - 1) / 2;
        AtomicInteger loopCounter = new AtomicInteger();
        AtomicInteger accumulator = new AtomicInteger();

        // 1. Successful execution.
        FutureHelpers.doWhileLoop(
                () -> {
                    int i = loopCounter.get();
                    accumulator.addAndGet(i);
                    return CompletableFuture.completedFuture(loopCounter.incrementAndGet());
                },
                x -> x < maxLoops
        ).join();

        Assert.assertEquals("Unexpected result for loop without a specific accumulator.", expectedResult,
                accumulator.get());

        //2. With exceptions.
        loopCounter.set(0);
        accumulator.set(0);
        CompletableFuture<Void> loopFuture = FutureHelpers.doWhileLoop(
                () -> {
                    if (loopCounter.incrementAndGet() % 3 == 0) {
                        throw new IntentionalException();
                    } else {
                        accumulator.addAndGet(loopCounter.get());
                        return CompletableFuture.completedFuture(loopCounter.get());
                    }
                },
                x -> x < maxLoops);

        AssertExtensions.assertThrows(
                "doWhileLoop() did not return a failed Future when one of the loopBody calls returned a failed Future.",
                loopFuture::join,
                ex -> ex instanceof IntentionalException);
        Assert.assertEquals("Unexpected value accumulated until loop was interrupted.", 3, accumulator.get());
    }

    private List<CompletableFuture<Integer>> createNumericFutures(int count) {
        ArrayList<CompletableFuture<Integer>> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(new CompletableFuture<>());
        }

        return result;
    }

    private void completeFutures(List<CompletableFuture<Integer>> futures) {
        for (int i = 0; i < futures.size(); i++) {
            if (!futures.get(i).isDone()) {
                futures.get(i).complete(i); // It may have previously been completed exceptionally.
            }
        }
    }

    private void failRandomFuture(List<CompletableFuture<Integer>> futures) {
        int index = new Random().nextInt(futures.size());
        futures.get(index).completeExceptionally(new IntentionalException());
    }

    private void checkResults(Collection<Integer> results) {
        int expected = 0;
        for (int result : results) {
            Assert.assertEquals("Unexpected result for future " + expected, expected, result);
            expected++;
        }
    }
}
