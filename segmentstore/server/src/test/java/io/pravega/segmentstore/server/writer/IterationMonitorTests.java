/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.writer;

import io.pravega.test.common.InlineExecutor;
import io.pravega.test.common.IntentionalException;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Delayed;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link IterationMonitor} class.
 */
public class IterationMonitorTests {
    private static final Duration CHECK_INTERVAL = Duration.ofSeconds(10);
    private static final Duration MAX_IDLE = Duration.ofSeconds(30);

    /**
     * Tests normal functionality.
     */
    @Test
    public void testFunctionality() {
        val iterationCount = 10;
        val timeIncrement = MAX_IDLE.toMillis() / iterationCount;
        val delayFuture = new AtomicReference<>(new CompletableFuture<Void>());
        val idleDuration = new AtomicReference<Duration>(Duration.ZERO);
        val callbackCount = new AtomicInteger();
        @Cleanup("shutdown")
        val executor = new MockExecutor(delayFuture::get);
        @Cleanup
        val monitor = new IterationMonitor(idleDuration::get, CHECK_INTERVAL, MAX_IDLE, callbackCount::incrementAndGet, executor);

        int expectedInvocationCount = 0;
        for (int iterationId = 0; iterationId < iterationCount; iterationId++) {
            idleDuration.set(Duration.ofMillis(MAX_IDLE.toMillis() / 2 + timeIncrement * iterationId));
            if (idleDuration.get().compareTo(MAX_IDLE) >= 0) {
                expectedInvocationCount++;
            }

            delayFuture.getAndSet(new CompletableFuture<>()).complete(null); // Simulate timer elapsed.
            Assert.assertEquals("Unexpected invocation count for iteration " + iterationId, expectedInvocationCount, callbackCount.get());
        }

        // Close the monitor and verify that nothing else gets invoked.
        monitor.close();
        delayFuture.getAndSet(null).complete(null); // Simulate timer elapsed.
        Assert.assertEquals("Unexpected invocation count after closing.", expectedInvocationCount, callbackCount.get());
    }

    /**
     * Tests the ability to handle errors thrown by the callback.
     */
    @Test
    public void testCallbackErrorAsync() {
        val delayFuture = new AtomicReference<>(new CompletableFuture<Void>());
        val idleDuration = new AtomicReference<Duration>(MAX_IDLE);
        val callbackCount = new AtomicInteger();
        @Cleanup("shutdown")
        val executor = new MockExecutor(delayFuture::get);
        @Cleanup
        val monitor = new IterationMonitor(idleDuration::get, CHECK_INTERVAL, MAX_IDLE,
                () -> {
                    callbackCount.incrementAndGet();
                    throw new IntentionalException();
                }, executor);

        // Invoke first time.
        delayFuture.getAndSet(new CompletableFuture<>()).complete(null); // Simulate timer elapsed.
        Assert.assertEquals("Unexpected invocation count.", 1, callbackCount.get());

        // Invoke the second time. This is to make sure that the exception thrown the first time does not stall the monitor.
        delayFuture.getAndSet(new CompletableFuture<>()).complete(null); // Simulate timer elapsed.
        Assert.assertEquals("Unexpected invocation count.", 2, callbackCount.get());
    }

    /**
     * Tests the ability to cancel any ongoing monitoring if {@link IterationMonitor#close()} is invoked.
     */
    @Test
    public void testClose() {
        val delayFuture = new AtomicReference<>(new CompletableFuture<Void>());
        val idleDuration = new AtomicReference<Duration>(MAX_IDLE);
        val callbackCount = new AtomicInteger();
        @Cleanup("shutdown")
        val executor = new MockExecutor(delayFuture::get);
        @Cleanup
        val monitor = new IterationMonitor(idleDuration::get, CHECK_INTERVAL, MAX_IDLE, callbackCount::incrementAndGet, executor);

        monitor.close();

        // Invoke and verify that it doesn't get run.
        delayFuture.getAndSet(new CompletableFuture<>()).complete(null); // Simulate timer elapsed.
        Assert.assertEquals("Unexpected invocation count.", 0, callbackCount.get());
    }

    //region MockExecutor and MockFuture

    @RequiredArgsConstructor
    private static class MockExecutor implements ScheduledExecutorService {
        private final Supplier<CompletableFuture<Void>> scheduleDelayProvider;
        private final InlineExecutor inlineExecutor = new InlineExecutor();

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable runnable, long initialDelayMillis, long delayMillis, TimeUnit timeUnit) {
            Assert.assertEquals("MockExecutor can only support millis.", TimeUnit.MILLISECONDS, timeUnit);
            val result = new MockFuture(delayMillis);
            runScheduledTask(runnable, result::isCancelled);
            return result;
        }

        private void runScheduledTask(Runnable runnable, Supplier<Boolean> isCancelled) {
            this.scheduleDelayProvider.get().thenRunAsync(() -> {
                if (isCancelled.get()) {
                    return;
                }

                runnable.run();
                runScheduledTask(runnable, isCancelled);
            }, this.inlineExecutor);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable runnable, long l, long l1, TimeUnit timeUnit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable runnable, long delayMillis, TimeUnit timeUnit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long l, TimeUnit timeUnit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
            this.inlineExecutor.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return this.inlineExecutor.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return this.inlineExecutor.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return this.inlineExecutor.isTerminated();
        }

        @Override
        public boolean awaitTermination(long l, TimeUnit timeUnit) {
            return false;
        }

        @Override
        public <T> Future<T> submit(Callable<T> callable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> Future<T> submit(Runnable runnable, T t) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Future<?> submit(Runnable runnable) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> collection) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void execute(Runnable runnable) {
            throw new UnsupportedOperationException();
        }
    }

    @RequiredArgsConstructor
    private static class MockFuture implements ScheduledFuture<Void> {
        private final long delay;
        private final AtomicBoolean cancelled = new AtomicBoolean();

        @Override
        public boolean cancel(boolean b) {
            return this.cancelled.compareAndSet(false, true);
        }

        @Override
        public boolean isCancelled() {
            return this.cancelled.get();
        }

        @Override
        public boolean isDone() {
            return this.cancelled.get();
        }

        @Override
        public long getDelay(TimeUnit timeUnit) {
            Assert.assertEquals("MockExecutor can only support millis.", TimeUnit.MILLISECONDS, timeUnit);
            return this.delay;
        }

        @Override
        public int compareTo(Delayed delayed) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void get() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Void get(long l, TimeUnit timeUnit) {
            throw new UnsupportedOperationException();
        }
    }

    //endregion
}
