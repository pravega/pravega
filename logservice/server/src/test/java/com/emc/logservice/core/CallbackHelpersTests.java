package com.emc.logservice.core;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Unit tests for CallbackHelpers class.
 */
public class CallbackHelpersTests {
    /**
     * Tests the CallbackHelpers.invokeSafely(Consumer) method.
     *
     */
    @Test
    public void testInvokeConsumer() {

        // Test happy case: no exception.
        AtomicInteger invokeCount = new AtomicInteger(0);
        Consumer<Integer> goodConsumer = v -> invokeCount.incrementAndGet();

        AtomicBoolean exceptionHandled = new AtomicBoolean(false);
        Consumer<Throwable> failureHandler = ex -> exceptionHandled.set(true);

        CallbackHelpers.invokeSafely(goodConsumer, 1, failureHandler);
        Assert.assertFalse("Exception handler was invoked when no exception was thrown.", exceptionHandled.get());
        Assert.assertEquals("Unexpected number of callback invocations.", 1, invokeCount.get());

        invokeCount.set(0);
        exceptionHandled.set(false);

        // Test exceptional case
        Consumer<Integer> badConsumer = v -> {
            throw new RuntimeException("intentional");
        };

        // With explicit callback.
        CallbackHelpers.invokeSafely(badConsumer, 1, failureHandler);
        Assert.assertTrue("Exception handler was not invoked when an exception was thrown.", exceptionHandled.get());
        Assert.assertEquals("Unexpected number of callback invocations.", 0, invokeCount.get());
        exceptionHandled.set(false);

        // With no callback.
        CallbackHelpers.invokeSafely(badConsumer, 1, null);
        Assert.assertEquals("Unexpected number of callback invocations.", 0, invokeCount.get());

        // With callback that throws exceptions.
        CallbackHelpers.invokeSafely(badConsumer, 1, ex -> {
            throw new IllegalArgumentException("intentional");
        });

        Assert.assertEquals("Unexpected number of callback invocations.", 0, invokeCount.get());
    }
}
