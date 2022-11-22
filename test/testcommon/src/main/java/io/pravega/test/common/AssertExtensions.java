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
package io.pravega.test.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.junit.Assert;

import static org.junit.Assert.assertEquals;

/**
 * Additional Assert Methods that are useful during testing.
 */
public class AssertExtensions {

    /**
     * Invokes `eval` in a loop over and over (with a small sleep) and asserts that the value eventually reaches `expected`.
     * @param <T>                   The type of the value to compare.
     * @param expected              The expected return value.
     * @param eval                  The function to test
     * @param timeoutMillis         The timeout in milliseconds after which an assertion error should be thrown.
     * @throws Exception            If the is an assertion error, and exception from `eval`, or the thread is interrupted.
     */
    public static <T> void assertEventuallyEquals(T expected, Callable<T> eval, long timeoutMillis) throws Exception {
        assertEventuallyEquals(expected, eval, 10, timeoutMillis);
    }

    /**
     * Invokes `eval` in a loop over and over (with a small sleep) and asserts that the value eventually reaches `expected`.
     * @param <T>                   The type of the value to compare.
     * @param expected              The expected return value.
     * @param eval                  The function to test
     * @param checkIntervalMillis   The number of milliseconds to wait between two checks.
     * @param timeoutMillis         The timeout in milliseconds after which an assertion error should be thrown.
     * @throws Exception            If there is an assertion error, and exception from `eval`, or the thread is interrupted.
     */
    private static <T> void assertEventuallyEquals(T expected, Callable<T> eval, int checkIntervalMillis, long timeoutMillis) throws Exception {
        T result = null;
        try {
            result = TestUtils.awaitEvaluateExpr(expected, eval, checkIntervalMillis, timeoutMillis);
        } catch (TimeoutException e) {
            throw e;
        }
        assertEquals(expected, result);
    }

    /**
     * Invokes `eval` in a loop over and over (with a small sleep) and asserts that the value eventually reaches `expected`.
     *
     * @param <T>                 The type of the value to compare.
     * @param message             The message ot include.
     * @param expected            The expected return value.
     * @param eval                The function to test
     * @param checkIntervalMillis The number of milliseconds to wait between two checks.
     * @param timeoutMillis       The timeout in milliseconds after which an assertion error should be thrown.
     * @throws Exception If the is an assertion error, and exception from `eval`, or the thread is interrupted.
     */
    public static <T> void assertEventuallyEquals(String message, T expected, Callable<T> eval, int checkIntervalMillis, long timeoutMillis) throws Exception {
        assertEventuallyEquals(expected, eval, checkIntervalMillis, timeoutMillis);
        assertEquals(message, expected, eval.call());
    }

    /**
     * Asserts that an exception of the Type provided is thrown eventually.
     *
     * @param run  The Runnable to execute.
     * @param type The type of exception to expect.
     * @param checkIntervalMillis The number of milliseconds to wait between two checks.
     * @param timeoutMillis       The timeout in milliseconds after which an assertion error should be thrown.
     * @throws Exception If the is an assertion error, and exception from `eval`, or the thread is interrupted.
     */
    public static void assertEventuallyThrows(Class<? extends Exception> type, RunnableWithException run, int checkIntervalMillis, long timeoutMillis) throws Exception {
        try {
        TestUtils.awaitException(() -> {
                try {
                    run.run();
                    Assert.fail("No exception thrown where: " + type.getName() + " was expected");
                } catch (CompletionException | ExecutionException e) {
                    if (!type.isAssignableFrom(e.getCause().getClass())) {
                        throw new RuntimeException(
                                "Exception of the wrong type. Was expecting " + type + " but got: " + e.getCause().getClass().getName(),
                                e);
                    }
                } catch (Exception e) {
                    if (!type.isAssignableFrom(e.getClass())) {
                        throw new RuntimeException(
                                "Exception of the wrong type. Was expecting " + type + " but got: " + e.getClass().getName(),
                                e);
                    }
                    return true;
                }
            return false;
        }, checkIntervalMillis, timeoutMillis);
        } catch (TimeoutException e) {
            throw new TimeoutException("Expected exception did not occur:" + type.getName());
        }
    }

    /**
     * Asserts that an exception of the Type provided is thrown.
     *
     * @param run  The Runnable to execute.
     * @param type The type of exception to expect.
     */
    public static void assertThrows(Class<? extends Throwable> type, RunnableWithException run) {
        try {
            run.run();
            Assert.fail("No exception thrown where: " + type.getName() + " was expected");
        } catch (CompletionException | ExecutionException e) {
            if (!type.isAssignableFrom(e.getCause().getClass())) {
                throw new RuntimeException(
                        "Exception of the wrong type. Was expecting " + type + " but got: " + e.getCause().getClass().getName(),
                        e);
            }
        } catch (Exception e) {
            if (!type.isAssignableFrom(e.getClass())) {
                throw new RuntimeException(
                        "Exception of the wrong type. Was expecting " + type + " but got: " + e.getClass().getName(),
                        e);
            }
        }
    }

    /**
     * Asserts that a function throws an expected exception.
     *
     * @param message  The message to include in the Assert calls.
     * @param runnable The function to test.
     * @param tester   A predicate that indicates whether the exception (if thrown) is as expected.
     */
    public static void assertThrows(String message, RunnableWithException runnable, Predicate<Throwable> tester) {
        try {
            runnable.run();
            Assert.fail(message + " No exception has been thrown.");
        } catch (CompletionException | ExecutionException ex) {
            if (!tester.test(ex.getCause())) {
                throw new AssertionError(message + " Exception thrown failed tester: " + ex.getCause(), ex);
            }
        } catch (Throwable ex) {
            if (!tester.test(ex)) {
                throw new AssertionError(message + " Exception thrown failed tester: " + ex, ex);
            }
        }
    }

    /**
     * Asserts that a future (that has not yet been invoked) throws an expected exception.
     *
     * @param message        The message to include in the Assert calls.
     * @param futureSupplier A Supplier that returns a new CompletableFuture, to test.
     * @param tester         A predicate that indicates whether the exception (if thrown) is as expected.
     * @param <T>            The type of the future's result.
     */
    public static <T> void assertSuppliedFutureThrows(String message,
            Supplier<CompletableFuture<T>> futureSupplier, Predicate<Throwable> tester) {
        try {
            futureSupplier.get().join();
            Assert.fail(message + " No exception has been thrown.");
        } catch (CompletionException ex) {
            if (!tester.test(getRealException(ex))) {
                Assert.fail(message + " Exception thrown was of unexpected type: " + getRealException(ex));
            }
        } catch (Exception ex) {
            if (!tester.test(ex)) {
                Assert.fail(message + " Exception thrown was of unexpected type: " + ex);
            }
        }
    }

    /**
     * Asserts that a future throws an expected exception.
     *
     * @param message The message to include in the Assert calls.
     * @param future  A the CompletableFuture to test.
     * @param tester  A predicate that indicates whether the exception (if thrown) is as expected.
     * @param <T>     The type of the future's result.
     */
    public static <T> void assertFutureThrows(String message, CompletableFuture<T> future, Predicate<Throwable> tester) {
        try {
            future.join();
            Assert.fail(message + " No exception has been thrown.");
        } catch (CompletionException ex) {
            if (!tester.test(getRealException(ex))) {
                Assert.fail(message + " Exception thrown was of unexpected type: " + getRealException(ex));
            }
        } catch (Exception ex) {
            if (!tester.test(ex)) {
                Assert.fail(message + " Exception thrown was of unexpected type: " + ex);
            }
        }
    }

    /**
     * Asserts that the contents of the given arrays are the same.
     *
     * @param message The message to include in the Assert calls.
     * @param array1  The first array to check.
     * @param offset1 The offset within the first array to start checking at.
     * @param array2  The second array to check.
     * @param offset2 The offset within the second array to start checking at.
     * @param length  The number of elements to check.
     */
    public static void assertArrayEquals(String message, byte[] array1, int offset1, byte[] array2, int offset2, int length) {
        // TODO: This method could be reduced to a single line (and execute faster) if we could compile this project with JDK11.
        // We could do argument checks here, but the array access below will throw the appropriate exceptions if any of these args are out of bounds.
        for (int i = 0; i < length; i++) {
            if (array1[i + offset1] != array2[i + offset2]) {
                Assert.fail(String.format(
                        "%s Arrays differ at check-offset %d. Array1[%d]=%d, Array2[%d]=%d.",
                        message,
                        i,
                        offset1 + i,
                        array1[i + offset1],
                        offset2 + i,
                        array2[i + offset2]));
            }
        }
    }

    /**
     * Asserts that the contents of the given InputStream are the same.
     *
     * @param message   The message to include in the Assert calls.
     * @param s1        The first InputStream to check.
     * @param s2        The second InputStream to check.
     * @param maxLength The maximum number of bytes to check.
     * @throws IOException If unable to read from any of the given streams.
     */
    public static void assertStreamEquals(String message, InputStream s1, InputStream s2, int maxLength) throws IOException {
        int readSoFar = 0;
        while (readSoFar < maxLength) {
            int b1 = s1.read();
            int b2 = s2.read();
            if (b1 != b2) {
                // This also includes the case when one stream ends prematurely.
                Assert.fail(String.format("%s InputStreams differ at index %d. Expected %d, actual %d.", message, readSoFar, b1, b2));
            }

            readSoFar++;
            if (b1 < 0) {
                break; // We have reached the end of both streams.
            }
        }
    }

    /**
     * Asserts that the contents of the given collections are the same (in any order).
     *
     * @param message  The message to include in the Assert calls.
     * @param expected A collection to check against.
     * @param actual   The collection to check.
     * @param <T>      The type of the collection's elements.
     */
    public static <T extends Comparable<? super T>> void assertContainsSameElements(String message, Collection<T> expected, Collection<T> actual) {
        Assert.assertEquals(String.format("%s Collections differ in size.", message), expected.size(), actual.size());
        for (T e : expected) {
            if (!actual.contains(e)) {
                Assert.fail(String.format("%s Element %s does not exist.", message, e));
            }
        }
    }

    /**
     * Asserts that the contents of the given collections are the same (in any order).
     *
     * @param message    The message to include in the Assert calls.
     * @param expected   A collection to check against.
     * @param actual     The collection to check.
     * @param comparator A Comparator to use to compare elements of the two collections.
     * @param <T>        The type of the collection's elements.
     */
    public static <T> void assertContainsSameElements(String message, Collection<T> expected, Collection<T> actual, Comparator<T> comparator) {
        Assert.assertEquals(String.format("%s Collections differ in size.", message), expected.size(), actual.size());
        for (T e : expected) {
            boolean contains = false;
            for (T a : actual) {
                if (comparator.compare(e, a) == 0) {
                    contains = true;
                    break;
                }
            }

            if (!contains) {
                Assert.fail(String.format("%s Element %s does not exist.", message, e));
            }
        }
    }

    /**
     * Asserts that the contents of the given maps are the same.
     *
     * @param message  The message to include in the Assert calls.
     * @param expected A Map to check against.
     * @param actual   The Map to check.
     * @param <K>      The type of the map's keys.
     * @param <V>      The type of the map's values.
     */
    public static <K, V> void assertMapEquals(String message, Map<K, V> expected, Map<K, V> actual) {
        Assert.assertEquals(String.format("%s Maps differ in size.", message), expected.size(), actual.size());
        for (Map.Entry<K, V> e : expected.entrySet()) {
            if (!actual.containsKey(e.getKey())) {
                Assert.fail(String.format("%s Element with key %s does not exist.", message, e.getKey()));
            }

            Assert.assertEquals(String.format("%s Element values for key %s differ.", message, e.getKey()), e.getValue(), actual.get(e.getKey()));
        }
    }

    /**
     * Asserts that the given lists contain equivalent elements (at the same indices), based on the given tester function.
     *
     * @param message  The message to include in the Assert calls.
     * @param expected The list to check against.
     * @param actual   The list to check.
     * @param tester   A BiPredicate that will be used to verify the elements at the same indices in each list are equivalent.
     * @param <T>      The type of the list's elements.
     */
    public static <T> void assertListEquals(String message, List<? extends T> expected, List<? extends T> actual, BiPredicate<T, T> tester) {
        Assert.assertEquals(String.format("%s Collections differ in size.", message), expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            T expectedItem = expected.get(i);
            T actualItem = actual.get(i);
            Assert.assertTrue(String.format("%s Elements at index %d differ. Expected '%s', found '%s'.", message, i, expectedItem, actualItem),
                    (expectedItem == null && actualItem == null) || tester.test(expectedItem, actualItem));
        }
    }

    /**
     * Determines whether two list contain the same elements.
     *
     * @param expected The list to check against.
     * @param actual   The list to check.
     * @param tester   A BiPredicate that will be used to verify the elements at the same indices in each list are equivalent.
     * @param <T>      The type of the list's elements.
     * @return True if the lists are equal, false otherwise.
     */
    public static <T> boolean listEquals(List<T> expected, List<T> actual, BiPredicate<T, T> tester) {
        if (expected.size() != actual.size()) {
            return false;
        }
        for (int i = 0; i < expected.size(); i++) {
            T expectedItem = expected.get(i);
            T actualItem = actual.get(i);
            if (!tester.test(expectedItem, actualItem)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Asserts that actual {@literal <} expected.
     *
     * @param message  The message to include in the Assert calls.
     * @param expected The larger value.
     * @param actual   The smaller value.
     */
    public static void assertLessThan(String message, long expected, long actual) {
        Assert.assertTrue(String.format("%s Expected: less than %d. Actual: %d.", message, expected, actual), expected > actual);
    }

    /**
     * Asserts that actual {@literal <=} expected.
     *
     * @param message  The message to include in the Assert calls.
     * @param expected The larger value.
     * @param actual   The smaller value.
     */
    public static void assertLessThanOrEqual(String message, long expected, long actual) {
        Assert.assertTrue(String.format("%s Expected: less than or equal to %d. Actual: %d.", message, expected, actual), expected >= actual);
    }

    /**
     * Asserts that actual {@literal >} expected.
     *
     * @param message  The message to include in the Assert calls.
     * @param expected The smaller value.
     * @param actual   The larger value.
     */
    public static void assertGreaterThan(String message, long expected, long actual) {
        Assert.assertTrue(String.format("%s Expected: greater than %d. Actual: %d.", message, expected, actual), expected < actual);
    }

    /**
     * Asserts that actual {@literal >=} expected.
     *
     * @param message  The message to include in the Assert calls.
     * @param expected The smaller value.
     * @param actual   The larger value.
     */
    public static void assertGreaterThanOrEqual(String message, long expected, long actual) {
        Assert.assertTrue(String.format("%s Expected: greater than or equal to %d. Actual: %d.", message, expected, actual), expected <= actual);
    }

    /**
     * Asserts that string is null or equal to the empty string.
     *
     * @param message The message to include in the Assert calls.
     * @param string  The String to test.
     */
    public static void assertNullOrEmpty(String message, String string) {
        Assert.assertTrue(message, string == null || string.length() == 0);
    }

    /**
     * Asserts that string is not null or an empty string.
     *
     * @param message The message to include in the Assert calls.
     * @param string  The String to test.
     */
    public static void assertNotNullOrEmpty(String message, String string) {
        Assert.assertFalse(message, string == null || string.length() == 0);
    }

    /**
     * Asserts that a future (that has not yet been invoked) throws an expected exception or completes without
     * exception.
     *
     * @param message        The message to include in the Assert calls.
     * @param futureSupplier A Supplier that returns a new CompletableFuture, to test.
     * @param tester         A predicate that indicates whether the exception (if thrown) is as expected.
     * @param <T>            The type of the future's result.
     */
    public static <T> void assertMayThrow(String message, Supplier<CompletableFuture<T>> futureSupplier,
                                             Predicate<Throwable> tester) {
        try {
            futureSupplier.get().join();
        } catch (CompletionException ex) {
            if (!tester.test(getRealException(ex))) {
                Assert.fail(message + " Exception thrown was of unexpected type: " + getRealException(ex));
            }
        } catch (Exception ex) {
            if (!tester.test(ex)) {
                Assert.fail(message + " Exception thrown was of unexpected type: " + ex);
            }
        }
    }

    private static Throwable getRealException(Throwable ex) {
        if (ex instanceof CompletionException) {
            return getRealException(ex.getCause());
        }

        if (ex instanceof ExecutionException) {
            return getRealException(ex.getCause());
        }

        return ex;
    }

    @FunctionalInterface
    public interface RunnableWithException {
        void run() throws Exception;
    }

    /**
     * Asserts that the provided function blocks until the second function is run.
     *
     * @param blockingFunction The function that is expected to block
     * @param unblocker The function that is expected to unblock the blocking function.
     * @return The result of the blockingFunction.
     * @param <ResultT> The result of the blockingFunction.
     */
    public static <ResultT> ResultT assertBlocks(Callable<ResultT> blockingFunction, RunnableWithException unblocker) {
        final AtomicReference<ResultT> result = new AtomicReference<>(null);
        final AtomicReference<Throwable> exception = new AtomicReference<>(null);
        final Semaphore isBlocked = new Semaphore(0);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    result.set(blockingFunction.call());
                } catch (Throwable e) {
                    exception.set(e);
                }
                isBlocked.release();
            }
        });
        t.start();
        try {
            Assert.assertFalse(isBlocked.tryAcquire(200, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        try {
            unblocker.run();
        } catch (Exception e) {
            throw new RuntimeException("Blocking call threw an exception", e);
        }
        try {
            isBlocked.acquire();
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (exception.get() != null) {
            throw new RuntimeException(exception.get());
        } else {
            return result.get();
        }
    }

    /**
     * Asserts that the provided function blocks until the second function is run.
     *
     * @param blockingFunction The function that is expected to block
     * @param unblocker The function that is expected to unblock the blocking function.
     */
    public static void assertBlocks(RunnableWithException blockingFunction, RunnableWithException unblocker) {
        final AtomicReference<Throwable> exception = new AtomicReference<>(null);
        final Semaphore isBlocked = new Semaphore(0);
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    blockingFunction.run();
                } catch (Throwable e) {
                    exception.set(e);
                }
                isBlocked.release();
            }
        });
        t.start();
        try {
            if (isBlocked.tryAcquire(200, TimeUnit.MILLISECONDS)) {
                if (exception.get() != null) {
                    throw new RuntimeException("Blocking code threw an exception", exception.get());
                } else {
                    throw new AssertionError("Failed to block.");
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        try {
            unblocker.run();
        } catch (Exception e) {
            throw new RuntimeException("Blocking call threw an exception", e);
        }
        try {
            if (!isBlocked.tryAcquire(2000, TimeUnit.MILLISECONDS)) {
                RuntimeException e = new RuntimeException("Failed to unblock");
                e.setStackTrace(t.getStackTrace());
                t.interrupt();
                throw new RuntimeException(e);
            }
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        if (exception.get() != null) {
            throw new RuntimeException(exception.get());
        }
    }

    /**
    * Compares two floating point values with a given precision.
    *
    * @param a the first operand.
    * @param b the second operand.
    * @param precision the maximum absolute difference between the two values.
    * @return true if the two operands are both null or the represent the same
    * value within the given precision
    */
    public static boolean nearlyEquals(Double a, Double b, double precision) {
        if (a == null && b == null) {
            return true;
        }
        if (a != null && b != null) {
            return Math.abs(a - b) <= precision;
        }
        return false;
    }

    /**
     * Same as {@link Assert#fail(String)}, but this is a method that returns a value. Useful for more compact syntax
     * in lambdas that should return a value but only verify that nothing is invoked.
     *
     * @param message Message to display.
     * @param <T>     A type.
     * @return Nothing. This method always throws {@link AssertionError}.
     */
    public static <T> T fail(String message) {
        Assert.fail(message);
        return null;
    }
}
