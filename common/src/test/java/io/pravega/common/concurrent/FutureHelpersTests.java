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

import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import lombok.val;
import org.junit.Assert;
import org.junit.Test;

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
        AssertExtensions.assertThrows(
                "failedFuture() did not complete the future with the expected exception.",
                cf::join,
                e -> e.equals(ex));
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
        Assert.assertNull("exceptionListener invoked the callback when the future was completed normally.", thrownException.get());

        thrownException.set(null);
        cf = new CompletableFuture<>();
        Exception ex = new IntentionalException();
        FutureHelpers.exceptionListener(cf, thrownException::set);
        cf.completeExceptionally(ex);
        Assert.assertNotNull("exceptionListener did not invoke the callback when the future was completed exceptionally.", thrownException.get());
        Assert.assertEquals("Unexpected exception was passed to the callback from exceptionListener when the future was completed exceptionally.", ex, thrownException.get());
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
        Assert.assertTrue("allOf() did not create a completed future when all futures were previously complete.", allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());

        // Not completed futures.
        futures = createNumericFutures(count);
        allFuturesComplete = FutureHelpers.allOf(futures);
        Assert.assertFalse("allOf() created a completed future when none of the futures were previously complete.", allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOf() complete when all its futures completed.", allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());

        // At least one failed & completed future.
        futures = createNumericFutures(count);
        failRandomFuture(futures);
        allFuturesComplete = FutureHelpers.allOf(futures);
        Assert.assertFalse("allOf() created a completed future when not all of the futures were previously complete (but one failed).", allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOf() did not complete exceptionally when at least one of the futures failed.", allFuturesComplete.isCompletedExceptionally());

        // At least one failed future.
        futures = createNumericFutures(count);
        allFuturesComplete = FutureHelpers.allOf(futures);
        failRandomFuture(futures);
        Assert.assertFalse("The result of allOf() completed when not all the futures completed (except one that failed).", allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOf() did not complete exceptionally when at least one of the futures failed.", allFuturesComplete.isCompletedExceptionally());
    }

    /**
     * Tests the allOfWithResults(List) method.
     */
    @Test
    public void testAllOfWithResultsList() {
        int count = 10;

        // Already completed futures.
        List<CompletableFuture<Integer>> futures = createNumericFutures(count);
        completeFutures(futures);
        CompletableFuture<List<Integer>> allFuturesComplete = FutureHelpers.allOfWithResults(futures);
        Assert.assertTrue("allOfWithResults() did not create a completed future when all futures were previously complete.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkResults(allFuturesComplete.join());

        // Not completed futures.
        futures = createNumericFutures(count);
        allFuturesComplete = FutureHelpers.allOfWithResults(futures);
        Assert.assertFalse("allOfWithResults() created a completed future when none of the futures were previously complete.", allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOfWithResults() complete when all its futures completed.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkResults(allFuturesComplete.join());

        // At least one failed & completed future.
        futures = createNumericFutures(count);
        failRandomFuture(futures);
        allFuturesComplete = FutureHelpers.allOfWithResults(futures);
        Assert.assertFalse("allOfWithResults() created a completed future when not all of the futures were previously complete (but one failed).",
                allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOfWithResults() did not complete exceptionally when at least one of the futures failed.",
                allFuturesComplete.isCompletedExceptionally());

        // At least one failed future.
        futures = createNumericFutures(count);
        allFuturesComplete = FutureHelpers.allOfWithResults(futures);
        failRandomFuture(futures);
        Assert.assertFalse("The result of allOfWithResults() completed when not all the futures completed (except one that failed).",
                allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOfWithResults() did not complete exceptionally when at least one of the futures failed.",
                allFuturesComplete.isCompletedExceptionally());
    }

    /**
     * Tests the allOfWithResults(Map) method.
     */
    @Test
    public void testAllOfWithResultsMap() {
        int count = 10;

        // Already completed futures.
        Map<Integer, CompletableFuture<Integer>> futures = createMappedNumericFutures(count);
        completeFutures(futures);
        CompletableFuture<Map<Integer, Integer>> allFuturesComplete = FutureHelpers.allOfWithResults(futures);
        Assert.assertTrue("allOfWithResults() did not create a completed future when all futures were previously complete.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkResults(allFuturesComplete.join());

        // Not completed futures.
        futures = createMappedNumericFutures(count);
        allFuturesComplete = FutureHelpers.allOfWithResults(futures);
        Assert.assertFalse("allOfWithResults() created a completed future when none of the futures were previously complete.",
                allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOfWithResults() complete when all its futures completed.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkResults(allFuturesComplete.join());

        // At least one failed & completed future.
        futures = createMappedNumericFutures(count);
        failRandomFuture(new ArrayList<>(futures.values()));
        allFuturesComplete = FutureHelpers.allOfWithResults(futures);
        Assert.assertFalse("allOfWithResults() created a completed future when not all of the futures were previously complete (but one failed).",
                allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOfWithResults() did not complete exceptionally when at least one of the futures failed.",
                allFuturesComplete.isCompletedExceptionally());

        // At least one failed future.
        futures = createMappedNumericFutures(count);
        allFuturesComplete = FutureHelpers.allOfWithResults(futures);
        failRandomFuture(new ArrayList<>(futures.values()));
        Assert.assertFalse("The result of allOfWithResults() completed when not all the futures completed (except one that failed).",
                allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOfWithResults() did not complete exceptionally when at least one of the futures failed.",
                allFuturesComplete.isCompletedExceptionally());
    }

    /**
     * Tests the allOfWithResults(Map) method.
     */
    @Test
    public void testKeysAllOfWithResults() {
        int count = 10;

        // Already completed futures.
        Map<CompletableFuture<Integer>, Integer> futures = createMappedNumericKeyFutures(count);
        completeKeyFutures(futures);
        CompletableFuture<Map<Integer, Integer>> allFuturesComplete = FutureHelpers.keysAllOfWithResults(futures);
        Assert.assertTrue("keysAllOfWithResults() did not create a completed future when all futures were previously complete.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkKeyResults(allFuturesComplete.join());

        // Not completed futures.
        futures = createMappedNumericKeyFutures(count);
        allFuturesComplete = FutureHelpers.keysAllOfWithResults(futures);
        Assert.assertFalse("keysAllOfWithResults() created a completed future when none of the futures were previously complete.",
                allFuturesComplete.isDone());
        completeKeyFutures(futures);
        Assert.assertTrue("The result of keysAllOfWithResults() complete when all its futures completed.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkKeyResults(allFuturesComplete.join());

        // At least one failed & completed future.
        futures = createMappedNumericKeyFutures(count);
        failRandomFuture(new ArrayList<>(futures.keySet()));
        allFuturesComplete = FutureHelpers.keysAllOfWithResults(futures);
        Assert.assertFalse("keysAllOfWithResults() created a completed future when not all of the futures were previously complete (but one failed).",
                allFuturesComplete.isDone());
        completeKeyFutures(futures);
        Assert.assertTrue("The result of keysAllOfWithResults() did not complete exceptionally when at least one of the futures failed.",
                allFuturesComplete.isCompletedExceptionally());

        // At least one failed future.
        futures = createMappedNumericKeyFutures(count);
        allFuturesComplete = FutureHelpers.keysAllOfWithResults(futures);
        failRandomFuture(new ArrayList<>(futures.keySet()));
        Assert.assertFalse("The result of keysAllOfWithResults() completed when not all the futures completed (except one that failed).",
                allFuturesComplete.isDone());
        completeKeyFutures(futures);
        Assert.assertTrue("The result of keysAllOfWithResults() did not complete exceptionally when at least one of the futures failed.",
                allFuturesComplete.isCompletedExceptionally());
    }

    /**
     * Test method for FutureHelpers.filter.
     *
     * @throws InterruptedException when future is interrupted
     * @throws ExecutionException   when future is interrupted
     */
    @Test
    public void testFilter() throws ExecutionException, InterruptedException {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);

        Predicate<Integer> evenFilter = (Integer x) -> x % 2 == 0;
        Function<Integer, CompletableFuture<Boolean>> futureEvenFilter = x -> CompletableFuture.completedFuture(x % 2 == 0);

        CompletableFuture<List<Integer>> filteredList = FutureHelpers.filter(list, futureEvenFilter);

        Assert.assertEquals("Unexpected filtered list size.", filteredList.get().size(), 3);
        Assert.assertEquals("Unexpected filtered list contents.", filteredList.get(), list.stream().filter(evenFilter).collect(Collectors.toList()));
    }

    /**
     * Test method for FutureHelpers.filter when the FuturePredicate completes exceptionally in future.
     */
    @Test
    public void testFilterException() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);

        Function<Integer, CompletableFuture<Boolean>> futureEvenFilter =
                x -> FutureHelpers.failedFuture(new IntentionalException("intentional"));

        AssertExtensions.assertThrows(
                "Unexpected behavior when filter threw an exception.",
                () -> FutureHelpers.filter(list, futureEvenFilter),
                ex -> ex instanceof IntentionalException);
    }

    @Test
    public void testCompleteAfter() {
        // Async exceptions (before call to completeAfter).
        val toFail1 = new CompletableFuture<Integer>();
        FutureHelpers.completeAfter(() -> FutureHelpers.failedFuture(new IntentionalException()), toFail1);
        Assert.assertTrue("Async exceptions were not propagated properly (before).", toFail1.isCompletedExceptionally());
        AssertExtensions.assertThrows(
                "Unexpected async exception got propagated (before).",
                toFail1::join,
                ex -> ex instanceof IntentionalException);

        // Async exceptions (after call to completeAfter).
        val sourceToFail2 = new CompletableFuture<Integer>();
        val toFail2 = new CompletableFuture<Integer>();
        FutureHelpers.completeAfter(() -> sourceToFail2, toFail2);
        sourceToFail2.completeExceptionally(new IntentionalException());
        Assert.assertTrue("Async exceptions were not propagated properly (after).", toFail2.isCompletedExceptionally());
        AssertExtensions.assertThrows(
                "Unexpected async exception got propagated (after).",
                toFail2::join,
                ex -> ex instanceof IntentionalException);

        // Sync exceptions.
        val toFail3 = new CompletableFuture<Integer>();
        AssertExtensions.assertThrows(
                "Sync exception did not get rethrown.",
                () -> FutureHelpers.completeAfter(() -> {
                    throw new IntentionalException();
                }, toFail3),
                ex -> ex instanceof IntentionalException);
        Assert.assertTrue("Sync exceptions were not propagated properly.", toFail3.isCompletedExceptionally());
        AssertExtensions.assertThrows(
                "Unexpected sync exception got propagated.",
                toFail3::join,
                ex -> ex instanceof IntentionalException);

        // Normal completion (before call to completeAfter).
        val toComplete1 = new CompletableFuture<Integer>();
        FutureHelpers.completeAfter(() -> CompletableFuture.completedFuture(1), toComplete1);
        Assert.assertTrue("Normal completion did not happen (before).", FutureHelpers.isSuccessful(toComplete1));
        Assert.assertEquals("Unexpected value from normal completion (before).", 1, (int) toComplete1.join());

        // Normal completion (after call to completeAfter).
        val sourceToComplete2 = new CompletableFuture<Integer>();
        val toComplete2 = new CompletableFuture<Integer>();
        FutureHelpers.completeAfter(() -> sourceToComplete2, toComplete2);
        sourceToComplete2.complete(2);
        Assert.assertTrue("Normal completion did not happen (after).", FutureHelpers.isSuccessful(toComplete2));
        Assert.assertEquals("Unexpected value from normal completion (after).", 2, (int) toComplete2.join());
    }

    @Test
    public void testLoop() {
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
        Assert.assertEquals("Unexpected result for loop without a specific accumulator.", expectedResult, accumulator.get());

        //2. With specific accumulator.
        loopCounter.set(0);
        accumulator.set(0);
        FutureHelpers.loop(
                () -> loopCounter.incrementAndGet() < maxLoops,
                () -> CompletableFuture.completedFuture(loopCounter.get()),
                accumulator::addAndGet,
                ForkJoinPool.commonPool()).join();
        Assert.assertEquals("Unexpected result for loop with a specific accumulator.", expectedResult, accumulator.get());

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

        // 1. Verify this is actually a do-while loop vs a regular while loop.
        FutureHelpers.doWhileLoop(
                () -> {
                    accumulator.incrementAndGet();
                    return CompletableFuture.completedFuture(0);
                },
                x -> false, // Only one iteration.
                ForkJoinPool.commonPool()
        ).join();
        Assert.assertEquals("Unexpected result for loop without a specific accumulator.", 1, accumulator.get());

        // 2. Successful execution.
        loopCounter.set(0);
        accumulator.set(0);
        FutureHelpers.doWhileLoop(
                () -> {
                    int i = loopCounter.get();
                    accumulator.addAndGet(i);
                    return CompletableFuture.completedFuture(loopCounter.incrementAndGet());
                },
                x -> x < maxLoops,
                ForkJoinPool.commonPool()
        ).join();

        Assert.assertEquals("Unexpected result for loop without a specific accumulator.", expectedResult, accumulator.get());

        // 3. With exceptions.
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
                x -> x < maxLoops, ForkJoinPool.commonPool());

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

    private Map<Integer, CompletableFuture<Integer>> createMappedNumericFutures(int count) {
        HashMap<Integer, CompletableFuture<Integer>> result = new HashMap<>();
        for (int i = 0; i < count; i++) {
            result.put(i, new CompletableFuture<>());
        }

        return result;
    }

    private Map<CompletableFuture<Integer>, Integer> createMappedNumericKeyFutures(int count) {
        HashMap<CompletableFuture<Integer>, Integer> result = new HashMap<>();
        for (int i = 0; i < count; i++) {
            result.put( new CompletableFuture<>(), i);
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

    private void completeFutures(Map<Integer, CompletableFuture<Integer>> futures) {
        for (int i = 0; i < futures.size(); i++) {
            if (!futures.get(i).isDone()) {
                futures.get(i).complete(i * i); // It may have previously been completed exceptionally.
            }
        }
    }

    private void completeKeyFutures(Map<CompletableFuture<Integer>, Integer> futures) {
        futures.forEach((future, value) -> {
            if (!future.isDone()) {
                future.complete( value * value); // It may have previously been completed exceptionally.
            }
        } );
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

    private void checkResults(Map<Integer, Integer> results) {
        int expected = 0;
        for (Map.Entry<Integer, Integer> entry : results.entrySet()) {
            Assert.assertEquals("Unexpected result for future " + expected, entry.getKey() * entry.getKey(), (int) entry.getValue());
            expected++;
        }
    }

    private void checkKeyResults(Map<Integer, Integer> results) {
        int expected = 0;
        for (Map.Entry<Integer, Integer> entry : results.entrySet()) {
            Assert.assertEquals("Unexpected result for future " + expected, entry.getValue() * entry.getValue(), (int) entry
                    .getKey());
            expected++;
        }
    }
}
