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

import io.pravega.common.Exceptions;
import io.pravega.common.hash.RandomFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static io.pravega.common.concurrent.Futures.exceptionallyComposeExpecting;
import static io.pravega.common.concurrent.Futures.exceptionallyExpecting;
import static io.pravega.common.concurrent.Futures.getAndHandleExceptions;
import static io.pravega.common.concurrent.Futures.getThrowingException;
import static io.pravega.common.concurrent.Futures.getThrowingExceptionWithTimeout;
import static io.pravega.test.common.AssertExtensions.assertThrows;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Unit tests for the Futures class.
 */
public class FuturesTests extends ThreadPooledTestSuite {
    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Test
    public void testGetThrowingExceptions() {
        CompletableFuture<String> future = new CompletableFuture<String>();
        future.complete("success");
        assertEquals("success", getThrowingException(future));
        CompletableFuture<String> failedFuture  = new CompletableFuture<String>();
        failedFuture.completeExceptionally(new RuntimeException("fail"));
        assertThrows("",
                     () -> getThrowingException(failedFuture),
                     e -> e.getMessage().equals("fail") && e.getClass().equals(RuntimeException.class));
    }

    @Test
    public void testGetThrowingExceptionsWithTimeout() throws TimeoutException {
        CompletableFuture<String> future = new CompletableFuture<String>();
        future.complete("success");
        // It should successfully complete the future
        assertEquals("success", getThrowingExceptionWithTimeout(future, 10000));
        // Testing the failing case
        CompletableFuture<String> failedFuture  = new CompletableFuture<String>();
        failedFuture.completeExceptionally(new RuntimeException("fail"));

        assertThrows("",
                () -> getThrowingExceptionWithTimeout(failedFuture, 10000),
                e -> e.getMessage().equals("fail") && e.getClass().equals(RuntimeException.class));

        // This should throw timeoutExceptions as future is not completing anytime
        CompletableFuture<String> timeoutFuture = new CompletableFuture<String>();
        assertThrows(TimeoutException.class, () -> getThrowingExceptionWithTimeout(timeoutFuture, 100));
        // Testing interrupted exceptions
        Thread.currentThread().interrupt();
        assertThrows(InterruptedException.class, () -> getThrowingExceptionWithTimeout(timeoutFuture, 1000));
    }

    @Test(timeout = 10000)
    public void testGetAndHandleException() throws TimeoutException {
        CompletableFuture<String> future = new CompletableFuture<String>();
        future.complete("success");
        assertEquals("success", getAndHandleExceptions(future, RuntimeException::new));
        assertEquals("success", getAndHandleExceptions(future, RuntimeException::new, 1000, MILLISECONDS));
        CompletableFuture<String> failedFuture  = new CompletableFuture<String>();
        failedFuture.completeExceptionally(new IllegalArgumentException("fail"));
        assertThrows("",
                     () -> getAndHandleExceptions(failedFuture, RuntimeException::new),
                     e -> e.getMessage().equals("java.lang.IllegalArgumentException: fail") && e.getClass().equals(RuntimeException.class));
        assertThrows("",
                     () -> getAndHandleExceptions(failedFuture, RuntimeException::new, 1000, MILLISECONDS),
                     e -> e.getMessage().equals("java.lang.IllegalArgumentException: fail") && e.getClass().equals(RuntimeException.class));
        CompletableFuture<String> incompleteFuture = new CompletableFuture<String>();
        assertThrows(TimeoutException.class, () -> getAndHandleExceptions(incompleteFuture, RuntimeException::new, 10, MILLISECONDS));
        Thread.currentThread().interrupt();
        try {
            getAndHandleExceptions(incompleteFuture, RuntimeException::new);
            fail();
            Thread.sleep(1); //Here only to fix compiler error
        } catch (InterruptedException e) {
            assertEquals(true, Thread.interrupted());
        }
        Thread.currentThread().interrupt();
        try {
            getAndHandleExceptions(incompleteFuture, RuntimeException::new, 1000, MILLISECONDS);
            fail();
            Thread.sleep(1); //Here only to fix compiler error
        } catch (InterruptedException e) {
            assertEquals(true, Thread.interrupted());
        }
        assertFalse(Thread.currentThread().isInterrupted());
    }
    
    @Test
    public void testExceptionallyExpecting() {
        CompletableFuture<String> future = new CompletableFuture<String>();
        future.complete("success");
        assertEquals("success", exceptionallyExpecting(future, e -> e instanceof RuntimeException, "failed").join());
        future = new CompletableFuture<String>();
        future.completeExceptionally(new RuntimeException());
        assertEquals("failed", exceptionallyExpecting(future, e -> e instanceof RuntimeException, "failed").join());
    }
    
    @Test
    public void testExceptionallyComposeExpecting() {
        CompletableFuture<String> future = new CompletableFuture<String>();
        future.complete("success");
        assertEquals("success", exceptionallyComposeExpecting(future, e -> e instanceof RuntimeException, () -> completedFuture("failed")).join());
        future = new CompletableFuture<String>();
        future.completeExceptionally(new RuntimeException());
        assertEquals("failed", exceptionallyComposeExpecting(future, e -> e instanceof RuntimeException, () -> completedFuture("failed")).join());
    }
    
    @Test
    public void testJoin() throws TimeoutException {
        CompletableFuture<String> future = new CompletableFuture<String>();
        future.complete("success");
        assertEquals("success", Futures.join(future, 1000, TimeUnit.MILLISECONDS));
        CompletableFuture<String> failedFuture = new CompletableFuture<String>();
        failedFuture.completeExceptionally(new RuntimeException("fail"));
        assertThrows("",
                     () -> Futures.join(failedFuture, 1000, TimeUnit.MILLISECONDS),
                     e -> e.getMessage().equals("fail") && e.getClass().equals(RuntimeException.class));
        CompletableFuture<String> incompleteFuture = new CompletableFuture<String>();
        assertThrows(TimeoutException.class, () -> Futures.join(incompleteFuture, 10, TimeUnit.MILLISECONDS));
    }
    
    @Test
    public void testToVoid() {
        CompletableFuture<String> future = new CompletableFuture<String>();
        future.complete("success");
        assertEquals(null, Futures.toVoid(future).join());
        CompletableFuture<String> failedFuture = new CompletableFuture<String>();
        failedFuture.completeExceptionally(new RuntimeException("fail"));
        assertThrows("",
                     () -> Futures.toVoid(failedFuture).join(),
                     e -> e.getMessage().equals("fail") && e.getClass().equals(RuntimeException.class));
    }
    
    @Test
    public void testToVoidExpecting() {
        CompletableFuture<String> future = new CompletableFuture<String>();
        future.complete("success");
        assertEquals(null, Futures.toVoidExpecting(future, "success", RuntimeException::new).join());
        CompletableFuture<String> failedFuture = new CompletableFuture<String>();
        failedFuture.completeExceptionally(new RuntimeException("fail"));
        assertThrows("",
                     () -> Futures.toVoidExpecting(failedFuture, "success", RuntimeException::new).join(),
                     e -> e.getMessage().equals("fail") && e.getClass().equals(RuntimeException.class));
        CompletableFuture<String> wrongFuture = new CompletableFuture<String>();
        wrongFuture.complete("fail");
        assertThrows("",
                     () -> Futures.toVoidExpecting(wrongFuture, "success", RuntimeException::new).join(),
                     e -> e.getClass().equals(RuntimeException.class));
    }
    
    /**
     * Tests the failedFuture() method.
     */
    @Test
    public void testFailedFuture() {
        Throwable ex = new IntentionalException();
        CompletableFuture<Void> cf = Futures.failedFuture(ex);
        Assert.assertTrue("failedFuture() did not create a failed future.", cf.isCompletedExceptionally());
        AssertExtensions.assertThrows(
                "failedFuture() did not complete the future with the expected exception.",
                cf::join,
                e -> e.equals(ex));
    }

    /**
     * Tests {@link Futures#cancellableFuture}.
     */
    @Test
    public void testCancellableFuture() {
        // Null input.
        Assert.assertNull(Futures.cancellableFuture(null, AtomicInteger::incrementAndGet));

        // Input completes first.
        val f1 = new CompletableFuture<AtomicInteger>();
        val f1r1 = Futures.cancellableFuture(f1, i -> i.addAndGet(10));
        val f1r2 = Futures.cancellableFuture(f1, i -> i.addAndGet(100));
        f1.complete(new AtomicInteger(1));
        Assert.assertEquals(f1.join(), f1r1.join());
        f1r2.cancel(true);
        Assert.assertEquals(f1.join(), f1r2.join());
        Assert.assertEquals(1, f1.join().get());

        // Input is completed exceptionally.
        val f2 = new CompletableFuture<AtomicInteger>();
        val f2r1 = Futures.cancellableFuture(f2, i -> i.addAndGet(20));
        val f2r2 = Futures.cancellableFuture(f2, i -> i.addAndGet(200));
        f2.completeExceptionally(new IntentionalException());
        Assert.assertTrue(f2r1.isCompletedExceptionally() && Futures.getException(f2r1) instanceof IntentionalException);
        f2r2.cancel(true);
        Assert.assertTrue(f2r2.isCompletedExceptionally() && Futures.getException(f2r2) instanceof IntentionalException);

        // Result is cancelled.
        val f3 = new CompletableFuture<AtomicInteger>();
        val f3r1 = Futures.cancellableFuture(f3, i -> i.addAndGet(1000)); // This one won't be cancelled.
        val f3r2 = Futures.cancellableFuture(f3, i -> i.addAndGet(100)); // This one will be cancelled.

        f3r2.cancel(true);
        Assert.assertFalse(f3.isDone() || f3r1.isDone());
        f3.complete(new AtomicInteger(1));
        Assert.assertEquals(101, f3.join().get());
        Assert.assertEquals(f3.join(), f3r1.join());
    }

    /**
     * Tests the exceptionListener() method.
     */
    @Test
    public void testExceptionListener() {
        AtomicReference<Throwable> thrownException = new AtomicReference<>();
        CompletableFuture<Void> cf = new CompletableFuture<>();
        Futures.exceptionListener(cf, thrownException::set);
        cf.complete(null);
        Assert.assertNull("exceptionListener invoked the callback when the future was completed normally.", thrownException.get());

        thrownException.set(null);
        cf = new CompletableFuture<>();
        Exception ex = new IntentionalException();
        Futures.exceptionListener(cf, thrownException::set);
        cf.completeExceptionally(ex);
        Assert.assertNotNull("exceptionListener did not invoke the callback when the future was completed exceptionally.", thrownException.get());
        Assert.assertEquals("Unexpected exception was passed to the callback from exceptionListener when the future was completed exceptionally.", ex, thrownException.get());
    }

    /**
     * Tests the exceptionallyCompose method.
     */
    @Test
    public void testExceptionallyCompose() {
        // When applied to a CompletableFuture that completes normally.
        val successfulFuture = new CompletableFuture<Integer>();
        val f1 = Futures.<Integer>exceptionallyCompose(successfulFuture, ex -> CompletableFuture.completedFuture(2));
        successfulFuture.complete(1);
        Assert.assertEquals("Unexpected completion value for successful future.", 1, (int) f1.join());

        // When applied to a CompletableFuture that completes exceptionally.
        val failedFuture = new CompletableFuture<Integer>();
        val f2 = Futures.<Integer>exceptionallyCompose(failedFuture, ex -> CompletableFuture.completedFuture(2));
        failedFuture.completeExceptionally(new IntentionalException());
        Assert.assertEquals("Unexpected completion value for failed future that handled the exception.", 2, (int) f2.join());

        // When applied to a CompletableFuture that completes exceptionally and the handler also throws.
        val f3 = Futures.<Integer>exceptionallyCompose(failedFuture, ex -> {
            throw new IntentionalException();
        });
        AssertExtensions.assertSuppliedFutureThrows(
                "Unexpected completion for failed future whose handler also threw an exception.",
                () -> f3,
                ex -> ex instanceof IntentionalException);
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
        CompletableFuture<Void> allFuturesComplete = Futures.allOf(futures);
        Assert.assertTrue("allOf() did not create a completed future when all futures were previously complete.", allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());

        // Not completed futures.
        futures = createNumericFutures(count);
        allFuturesComplete = Futures.allOf(futures);
        Assert.assertFalse("allOf() created a completed future when none of the futures were previously complete.", allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOf() complete when all its futures completed.", allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());

        // At least one failed & completed future.
        futures = createNumericFutures(count);
        failRandomFuture(futures);
        allFuturesComplete = Futures.allOf(futures);
        Assert.assertFalse("allOf() created a completed future when not all of the futures were previously complete (but one failed).", allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOf() did not complete exceptionally when at least one of the futures failed.", allFuturesComplete.isCompletedExceptionally());

        // At least one failed future.
        futures = createNumericFutures(count);
        allFuturesComplete = Futures.allOf(futures);
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
        CompletableFuture<List<Integer>> allFuturesComplete = Futures.allOfWithResults(futures);
        Assert.assertTrue("allOfWithResults() did not create a completed future when all futures were previously complete.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkResults(allFuturesComplete.join());

        // Not completed futures.
        futures = createNumericFutures(count);
        allFuturesComplete = Futures.allOfWithResults(futures);
        Assert.assertFalse("allOfWithResults() created a completed future when none of the futures were previously complete.", allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOfWithResults() complete when all its futures completed.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkResults(allFuturesComplete.join());

        // At least one failed & completed future.
        futures = createNumericFutures(count);
        failRandomFuture(futures);
        allFuturesComplete = Futures.allOfWithResults(futures);
        Assert.assertFalse("allOfWithResults() created a completed future when not all of the futures were previously complete (but one failed).",
                allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOfWithResults() did not complete exceptionally when at least one of the futures failed.",
                allFuturesComplete.isCompletedExceptionally());

        // At least one failed future.
        futures = createNumericFutures(count);
        allFuturesComplete = Futures.allOfWithResults(futures);
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
        CompletableFuture<Map<Integer, Integer>> allFuturesComplete = Futures.allOfWithResults(futures);
        Assert.assertTrue("allOfWithResults() did not create a completed future when all futures were previously complete.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkResults(allFuturesComplete.join());

        // Not completed futures.
        futures = createMappedNumericFutures(count);
        allFuturesComplete = Futures.allOfWithResults(futures);
        Assert.assertFalse("allOfWithResults() created a completed future when none of the futures were previously complete.",
                allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOfWithResults() complete when all its futures completed.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkResults(allFuturesComplete.join());

        // At least one failed & completed future.
        futures = createMappedNumericFutures(count);
        failRandomFuture(new ArrayList<>(futures.values()));
        allFuturesComplete = Futures.allOfWithResults(futures);
        Assert.assertFalse("allOfWithResults() created a completed future when not all of the futures were previously complete (but one failed).",
                allFuturesComplete.isDone());
        completeFutures(futures);
        Assert.assertTrue("The result of allOfWithResults() did not complete exceptionally when at least one of the futures failed.",
                allFuturesComplete.isCompletedExceptionally());

        // At least one failed future.
        futures = createMappedNumericFutures(count);
        allFuturesComplete = Futures.allOfWithResults(futures);
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
        CompletableFuture<Map<Integer, Integer>> allFuturesComplete = Futures.keysAllOfWithResults(futures);
        Assert.assertTrue("keysAllOfWithResults() did not create a completed future when all futures were previously complete.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkKeyResults(allFuturesComplete.join());

        // Not completed futures.
        futures = createMappedNumericKeyFutures(count);
        allFuturesComplete = Futures.keysAllOfWithResults(futures);
        Assert.assertFalse("keysAllOfWithResults() created a completed future when none of the futures were previously complete.",
                allFuturesComplete.isDone());
        completeKeyFutures(futures);
        Assert.assertTrue("The result of keysAllOfWithResults() complete when all its futures completed.",
                allFuturesComplete.isDone() && !allFuturesComplete.isCompletedExceptionally());
        checkKeyResults(allFuturesComplete.join());

        // At least one failed & completed future.
        futures = createMappedNumericKeyFutures(count);
        failRandomFuture(new ArrayList<>(futures.keySet()));
        allFuturesComplete = Futures.keysAllOfWithResults(futures);
        Assert.assertFalse("keysAllOfWithResults() created a completed future when not all of the futures were previously complete (but one failed).",
                allFuturesComplete.isDone());
        completeKeyFutures(futures);
        Assert.assertTrue("The result of keysAllOfWithResults() did not complete exceptionally when at least one of the futures failed.",
                allFuturesComplete.isCompletedExceptionally());

        // At least one failed future.
        futures = createMappedNumericKeyFutures(count);
        allFuturesComplete = Futures.keysAllOfWithResults(futures);
        failRandomFuture(new ArrayList<>(futures.keySet()));
        Assert.assertFalse("The result of keysAllOfWithResults() completed when not all the futures completed (except one that failed).",
                allFuturesComplete.isDone());
        completeKeyFutures(futures);
        Assert.assertTrue("The result of keysAllOfWithResults() did not complete exceptionally when at least one of the futures failed.",
                allFuturesComplete.isCompletedExceptionally());
    }

    /**
     * Test method for Futures.filter.
     *
     * @throws InterruptedException when future is interrupted
     * @throws ExecutionException   when future is interrupted
     */
    @Test
    public void testFilter() throws ExecutionException, InterruptedException {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);

        Predicate<Integer> evenFilter = (Integer x) -> x % 2 == 0;
        Function<Integer, CompletableFuture<Boolean>> futureEvenFilter = x -> CompletableFuture.completedFuture(x % 2 == 0);

        CompletableFuture<List<Integer>> filteredList = Futures.filter(list, futureEvenFilter);

        Assert.assertEquals("Unexpected filtered list size.", filteredList.get().size(), 3);
        Assert.assertEquals("Unexpected filtered list contents.", filteredList.get(), list.stream().filter(evenFilter).collect(Collectors.toList()));
    }

    /**
     * Test method for Futures.filter when the FuturePredicate completes exceptionally in future.
     */
    @Test
    public void testFilterException() {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7);

        Function<Integer, CompletableFuture<Boolean>> futureEvenFilter =
                x -> Futures.failedFuture(new IntentionalException("intentional"));

        AssertExtensions.assertSuppliedFutureThrows(
                "Unexpected behavior when filter threw an exception.",
                () -> Futures.filter(list, futureEvenFilter),
                ex -> ex instanceof IntentionalException);
    }

    @Test
    public void testCompleteAfter() {
        // Async exceptions (before call to completeAfter).
        val toFail1 = new CompletableFuture<Integer>();
        Futures.completeAfter(() -> Futures.failedFuture(new IntentionalException()), toFail1);
        Assert.assertTrue("Async exceptions were not propagated properly (before).", toFail1.isCompletedExceptionally());
        AssertExtensions.assertThrows(
                "Unexpected async exception got propagated (before).",
                toFail1::join,
                ex -> ex instanceof IntentionalException);

        // Async exceptions (after call to completeAfter).
        val sourceToFail2 = new CompletableFuture<Integer>();
        val toFail2 = new CompletableFuture<Integer>();
        Futures.completeAfter(() -> sourceToFail2, toFail2);
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
                () -> Futures.completeAfter(() -> {
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
        Futures.completeAfter(() -> CompletableFuture.completedFuture(1), toComplete1);
        Assert.assertTrue("Normal completion did not happen (before).", Futures.isSuccessful(toComplete1));
        Assert.assertEquals("Unexpected value from normal completion (before).", 1, (int) toComplete1.join());

        // Normal completion (after call to completeAfter).
        val sourceToComplete2 = new CompletableFuture<Integer>();
        val toComplete2 = new CompletableFuture<Integer>();
        Futures.completeAfter(() -> sourceToComplete2, toComplete2);
        sourceToComplete2.complete(2);
        Assert.assertTrue("Normal completion did not happen (after).", Futures.isSuccessful(toComplete2));
        Assert.assertEquals("Unexpected value from normal completion (after).", 2, (int) toComplete2.join());
    }

    @Test
    public void testLoop() {
        final int maxLoops = 10;
        final int expectedResult = maxLoops * (maxLoops - 1) / 2;
        AtomicInteger loopCounter = new AtomicInteger();
        AtomicInteger accumulator = new AtomicInteger();

        // 1. With no specific accumulator.
        Futures.loop(
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
        Futures.loop(
                () -> loopCounter.incrementAndGet() < maxLoops,
                () -> CompletableFuture.completedFuture(loopCounter.get()),
                accumulator::addAndGet,
                ForkJoinPool.commonPool()).join();
        Assert.assertEquals("Unexpected result for loop with a specific accumulator.", expectedResult, accumulator.get());

        //3. With exceptions.
        loopCounter.set(0);
        accumulator.set(0);
        CompletableFuture<Void> loopFuture = Futures.loop(
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
    public void testLoopIterable() {
        val list = IntStream.range(1, 10000).boxed().collect(Collectors.toList());
        val processedList = Collections.synchronizedList(new ArrayList<Integer>());
        Futures.loop(
                list,
                item -> {
                    processedList.add(item);
                    return CompletableFuture.completedFuture(true);
                },
                ForkJoinPool.commonPool()).join();
        AssertExtensions.assertListEquals("Unexpected result.", list, processedList, Integer::equals);
    }

    @Test
    public void testDoWhileLoopWithCondition() {
        final int maxLoops = 10;
        final int expectedResult = maxLoops * (maxLoops - 1) / 2;
        AtomicInteger loopCounter = new AtomicInteger();
        AtomicInteger accumulator = new AtomicInteger();

        // 1. Verify this is actually a do-while loop vs a regular while loop.
        Futures.doWhileLoop(
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
        Futures.doWhileLoop(
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
        CompletableFuture<Void> loopFuture = Futures.doWhileLoop(
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

    @Test
    public void testHandleCompose() {
        // When applied to a CompletableFuture that completes normally.
        val successfulFuture = new CompletableFuture<Integer>();
        val f1 = Futures.<Integer, String>handleCompose(successfulFuture, (r, ex) -> CompletableFuture.completedFuture("2"));
        successfulFuture.complete(1);
        Assert.assertEquals("Unexpected completion value for successful future.", "2", f1.join());

        // When applied to a CompletableFuture that completes exceptionally.
        val failedFuture = new CompletableFuture<Integer>();
        val f2 = Futures.<Integer, Integer>handleCompose(failedFuture, (r, ex) -> CompletableFuture.completedFuture(2));
        failedFuture.completeExceptionally(new IntentionalException());
        Assert.assertEquals("Unexpected completion value for failed future that handled the exception.", 2, (int) f2.join());

        // When applied to a CompletableFuture that completes exceptionally and the handler also throws.
        val f3 = Futures.<Integer, Integer>handleCompose(failedFuture, (r, ex) -> {
            throw new IntentionalException();
        });
        AssertExtensions.assertSuppliedFutureThrows(
                "Unexpected completion for failed future whose handler also threw an exception.",
                () -> f3,
                ex -> ex instanceof IntentionalException);
    }

    @Test
    public void testTimeout() {
        Supplier<CompletableFuture<Integer>> supplier = CompletableFuture::new;
        CompletableFuture<Integer> f1 = Futures.futureWithTimeout(supplier, Duration.ofMillis(10), "", executorService());
        AssertExtensions.assertFutureThrows("Future should have timedout. ", f1, e -> Exceptions.unwrap(e) instanceof TimeoutException);
    }

    @Test
    public void testCompleteOn() {
        val successfulFuture = new CompletableFuture<Integer>();
        CompletableFuture<Integer> result = Futures.completeOn(successfulFuture, executorService());
        successfulFuture.complete(1);
        Assert.assertEquals("Expected completion value for successful future.", Integer.valueOf(1), result.join());

        val failedFuture = new CompletableFuture<Integer>();
        CompletableFuture<Integer> failedResult = Futures.completeOn(failedFuture, executorService());
        failedFuture.completeExceptionally(new IntentionalException());
        AssertExtensions.assertSuppliedFutureThrows(
                "Failed future throws exception.",
                () -> failedResult,
                ex -> ex instanceof IntentionalException);
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
        int index = RandomFactory.create().nextInt(futures.size());
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
