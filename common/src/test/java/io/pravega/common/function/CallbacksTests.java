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
package io.pravega.common.function;

import io.pravega.test.common.IntentionalException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for Callbacks class.
 */
public class CallbacksTests {

    @Test
    public void testInvokeRunnable() {

        // Test happy case: no exception.
        AtomicInteger invokeCount = new AtomicInteger(0);
        RunnableWithException goodRunnable = invokeCount::incrementAndGet;

        AtomicBoolean exceptionHandled = new AtomicBoolean(false);
        Consumer<Throwable> failureHandler = ex -> exceptionHandled.set(true);

        Callbacks.invokeSafely(goodRunnable, failureHandler);
        Assert.assertFalse("Exception handler was invoked when no exception was thrown.", exceptionHandled.get());
        Assert.assertEquals("Unexpected number of callback invocations.", 1, invokeCount.get());

        invokeCount.set(0);
        exceptionHandled.set(false);

        // Test exceptional case
        RunnableWithException badRunnable = () -> {
            throw new IntentionalException();
        };

        // With explicit callback.
        Callbacks.invokeSafely(badRunnable, failureHandler);
        Assert.assertTrue("Exception handler was not invoked when an exception was thrown.", exceptionHandled.get());
        Assert.assertEquals("Unexpected number of callback invocations.", 0, invokeCount.get());
        exceptionHandled.set(false);

        // With no callback.
        Callbacks.invokeSafely(badRunnable, null);
        Assert.assertEquals("Unexpected number of callback invocations.", 0, invokeCount.get());

        // With callback that throws exceptions.
        Callbacks.invokeSafely(badRunnable, ex -> {
            throw new IntentionalException();
        });

        Assert.assertEquals("Unexpected number of callback invocations.", 0, invokeCount.get());
    }

    /**
     * Tests the CallbackHelpers.invokeSafely(Consumer) method.
     */
    @Test
    public void testInvokeConsumer() {

        // Test happy case: no exception.
        AtomicInteger invokeCount = new AtomicInteger(0);
        Consumer<Integer> goodConsumer = v -> invokeCount.incrementAndGet();

        AtomicBoolean exceptionHandled = new AtomicBoolean(false);
        Consumer<Throwable> failureHandler = ex -> exceptionHandled.set(true);

        Callbacks.invokeSafely(goodConsumer, 1, failureHandler);
        Assert.assertFalse("Exception handler was invoked when no exception was thrown.", exceptionHandled.get());
        Assert.assertEquals("Unexpected number of callback invocations.", 1, invokeCount.get());

        invokeCount.set(0);
        exceptionHandled.set(false);

        // Test exceptional case
        Consumer<Integer> badConsumer = v -> {
            throw new RuntimeException("intentional");
        };

        // With explicit callback.
        Callbacks.invokeSafely(badConsumer, 1, failureHandler);
        Assert.assertTrue("Exception handler was not invoked when an exception was thrown.", exceptionHandled.get());
        Assert.assertEquals("Unexpected number of callback invocations.", 0, invokeCount.get());
        exceptionHandled.set(false);

        // With no callback.
        Callbacks.invokeSafely(badConsumer, 1, null);
        Assert.assertEquals("Unexpected number of callback invocations.", 0, invokeCount.get());

        // With callback that throws exceptions.
        Callbacks.invokeSafely(badConsumer, 1, ex -> {
            throw new IllegalArgumentException("intentional");
        });

        Assert.assertEquals("Unexpected number of callback invocations.", 0, invokeCount.get());
    }
}
