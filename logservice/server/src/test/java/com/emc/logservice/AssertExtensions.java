package com.emc.logservice;

import org.junit.Assert;

import java.util.concurrent.CompletionException;
import java.util.function.Predicate;

/**
 * Additional Assert Methods that are useful during testing.
 */
public class AssertExtensions {
    /**
     * Asserts that a function throws an expected exception.
     *
     * @param message The message to include in the Assert calls.
     * @param runnable The function to test.
     * @param tester A predicate that indicates whether the exception (if thrown) is as expected.
     */
    public static void assertThrows(String message, Runnable runnable, Predicate<Throwable> tester) {
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
}
