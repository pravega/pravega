package com.emc.nautilus.testcommon;

import org.junit.Assert;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Additional Assert Methods that are useful during testing.
 */
public class AssertExtensions {
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
        }
        catch (CompletionException ex) {
            if (!tester.test(ex.getCause())) {
                Assert.fail(message + " Exception thrown was of unexpected type: " + ex.getCause());
            }
        }
        catch (Exception ex) {
            if (!tester.test(ex)) {
                Assert.fail(message + " Exception thrown was of unexpected type: " + ex);
            }
        }
    }

    /**
     * Asserts that a future (that has not yet been invoked) throws an expected exception.
     *
     * @param message        The message to include in the Assert calls.
     * @param futureSupplier A Supplier that returns a new CompletableFuture, to test.
     * @param tester         A predicate that indicates whether the exception (if thrown) is as expected.
     * @param <T>
     */
    public static <T> void assertThrows(String message, Supplier<CompletableFuture<T>> futureSupplier, Predicate<Throwable> tester) {
        try {
            futureSupplier.get().join();
            Assert.fail(message + " No exception has been thrown.");
        }
        catch (CompletionException ex) {
            if (!tester.test(getRealException(ex))) {
                Assert.fail(message + " Exception thrown was of unexpected type: " + getRealException(ex));
            }
        }
        catch (Exception ex) {
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
     * @param <T>
     */
    public static <T> void assertThrows(String message, CompletableFuture<T> future, Predicate<Throwable> tester) {
        try {
            future.join();
            Assert.fail(message + " No exception has been thrown.");
        }
        catch (CompletionException ex) {
            if (!tester.test(getRealException(ex))) {
                Assert.fail(message + " Exception thrown was of unexpected type: " + getRealException(ex));
            }
        }
        catch (Exception ex) {
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
     */
    public static void assertStreamEquals(String message, InputStream s1, InputStream s2, int maxLength) throws IOException {
        int readSoFar = 0;
        while (readSoFar < maxLength) {
            int b1 = s1.read();
            int b2 = s2.read();
            Assert.assertEquals(message + " InputStreams differ at index " + readSoFar, b1, b2); // This also includes the case when one stream ends prematurely.
            readSoFar++;
            if (b1 < 0) {
                break;// We have reached the end of both streams.
            }
        }
    }

    /**
     * Asserts that smaller < larger
     *
     * @param message The message to include in the Assert calls.
     * @param smaller The first value (smaller).
     * @param larger  The second value (larger).
     */
    public static void assertLessThan(String message, long smaller, long larger) {
        Assert.assertTrue(String.format("%s Expected: less than %d. Actual: %d.", message, smaller, larger), smaller < larger);
    }

    /**
     * Asserts that smaller <= larger
     *
     * @param message The message to include in the Assert calls.
     * @param smaller The first value (smaller).
     * @param larger  The second value (larger).
     */
    public static void assertLessThanOrEqual(String message, long smaller, long larger) {
        Assert.assertTrue(String.format("%s Expected: less than or equal to %d. Actual: %d.", message, smaller, larger), smaller <= larger);
    }

    /**
     * Asserts that larger > smaller
     *
     * @param message The message to include in the Assert calls.
     * @param larger  The first value (larger).
     * @param smaller The second value (smaller).
     */
    public static void assertGreaterThan(String message, long larger, long smaller) {
        Assert.assertTrue(String.format("%s Expected: greater than %d. Actual: %d.", message, smaller, larger), larger > smaller);
    }

    /**
     * Asserts that larger >= smaller
     *
     * @param message The message to include in the Assert calls.
     * @param larger  The first value (larger).
     * @param smaller The second value (smaller).
     */
    public static void assertGreaterThanOrEqual(String message, long larger, long smaller) {
        Assert.assertTrue(String.format("%s Expected: greater than or equal to %d. Actual: %d.", message, smaller, larger), larger >= smaller);
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

    private static Throwable getRealException(Throwable ex) {
        if (ex instanceof CompletionException) {
            return getRealException(ex.getCause());
        }

        if (ex instanceof ExecutionException) {
            return getRealException(ex.getCause());
        }

        return ex;
    }

    public interface RunnableWithException {
        void run() throws Exception;
    }
}
