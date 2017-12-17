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
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the KeyedSynchronizer class.
 */
public class KeyedSynchronizerTests extends ThreadPooledTestSuite {
    private static final int MAX_THREAD_COUNT = 5;
    private static final int TIMEOUT_MILLIS = 5000;
    @Rule
    public Timeout globalTimeout = Timeout.millis(TIMEOUT_MILLIS);

    @Override
    protected int getThreadPoolSize() {
        return MAX_THREAD_COUNT;
    }

    /**
     * Tests the case when the same key is attempted to be locked by multiple threads.
     */
    @Test
    public void testLockSameKey() throws Exception {
        final int key = 1;
        val ks = new KeyedSynchronizer<Integer>();

        val waitOn = new CompletableFuture<Void>();
        val isLocked = new AtomicBoolean();
        val acquiredFirstTime = new CompletableFuture<Void>();
        val f1 = CompletableFuture.supplyAsync(() -> {
            try (val ignored = ks.lock(key)) {
                Assert.assertEquals("Unexpected number of locks acquired.", 1, ks.getLockCount());

                // Notify that we acquired the lock and hold it until instructed to do so.
                isLocked.set(true);
                acquiredFirstTime.complete(null);
                waitOn.join();
                isLocked.set(false);
                return true;
            }
        }, executorService());

        // Attach an exception listener to catch any assertion errors.
        f1.exceptionally(acquiredFirstTime::completeExceptionally);

        // Wait for the lock to be acquired, then attempt to re-acquire it.
        acquiredFirstTime.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        val f2 = CompletableFuture.runAsync(() -> {
            try (val ignored = ks.lock(key)) {
                // Verify that we were only able to enter here when the lock was released.
                Assert.assertFalse("Lock was acquired while still being held.", isLocked.get());
                Assert.assertEquals("Unexpected number of locks acquired.", 1, ks.getLockCount());
            }

        }, executorService());

        waitOn.complete(null);
        CompletableFuture.allOf(f1, f2).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected number of locks registered after all have been released.", 0, ks.getLockCount());
    }

    /**
     * Tests the case when a lock acquisition fails due to the current thread being interrupted.
     */
    @Test
    public void testLockAcquireInterrupted() throws Exception {
        final int key = 1;
        val ks = new KeyedSynchronizer<Integer>();

        val result = new CompletableFuture<Void>();
        try (val lock1 = ks.lock(key)) {
            val t = new Thread(() -> {
                try (val lock2 = ks.lock(key)) {
                    Assert.fail("Lock was acquired successfully.");
                } catch (Throwable ex) {
                    if (ex instanceof InterruptedException) {
                        result.complete(null);
                    } else {
                        result.completeExceptionally(ex);
                    }
                }
            });

            t.start();
            t.interrupt();
            t.join();
        }

        Assert.assertEquals("Unexpected number of locks registered after all have been released.", 0, ks.getLockCount());

        // This will throw any exception that we complete with.
        result.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * Tests the fact that locks are non-reentrant (same thread cannot re-acquire a lock it already holds).
     */
    @Test
    public void testNonReentrant() {
        final int key = 1;
        val ks = new KeyedSynchronizer<Integer>();
        try (val ignored = ks.lock(key)) {
            AssertExtensions.assertThrows(
                    "Unexpected behavior when attempting to re-lock the same key by the same thread.",
                    () -> ks.lock(key),
                    ex -> ex instanceof IllegalMonitorStateException);
            Assert.assertEquals("Unexpected number of locks acquired.", 1, ks.getLockCount());
        }

        Assert.assertEquals("Unexpected number of locks after all have been released.", 0, ks.getLockCount());
    }

    /**
     * Tests the fact that multiple keys can be locked at the same time (by multiple threads) - i.e., they do not
     * interfere with each other.
     */
    @Test
    public void testLockDifferentKeys() throws Exception {
        val ks = new KeyedSynchronizer<Integer>();
        val futures = new ArrayList<CompletableFuture<Boolean>>();
        val waitOn = new CompletableFuture<Void>();
        val allAcquired = new CompletableFuture<Void>();
        val allReleased = new CompletableFuture<Void>();
        for (int i = 0; i < MAX_THREAD_COUNT; i++) {
            val key = i;
            val f = CompletableFuture.supplyAsync(() -> {
                try (val ignored = ks.lock(key)) {
                    if (ks.getLockCount() == MAX_THREAD_COUNT) {
                        // Notify that all the locks have been acquired.
                        allAcquired.complete(null);
                    }

                    // Wait for a signal to release all.
                    waitOn.join();
                }

                if (ks.getLockCount() == 0) {
                    // Notify that all the locks have been released.
                    allReleased.complete(null);
                }
                return true;
            });
            futures.add(f);

            // Catch any unexpected errors here.
            f.exceptionally(allAcquired::completeExceptionally);
        }

        // Wait for all the locks to be acquired.
        allAcquired.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected number of locks acquired.", MAX_THREAD_COUNT, ks.getLockCount());
        waitOn.complete(null);
        allReleased.get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected number of locks after all have been released.", 0, ks.getLockCount());

        // Double-check that the futures actually completed correctly.
        Futures.allOf(futures).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    /**
     * Tests the fact that multiple keys can be locked at the same time by the same thread.
     */
    @Test
    public void testLockDifferentKeysSameThread() {
        val ks = new KeyedSynchronizer<Integer>();
        val locks = new ArrayList<KeyedSynchronizer<Integer>.LockDelegate>();
        for (int i = 0; i < MAX_THREAD_COUNT; i++) {
            locks.add(ks.lock(i));
            Assert.assertEquals("Unexpected number of locks acquired.", i + 1, ks.getLockCount());
        }

        locks.forEach(KeyedSynchronizer.LockDelegate::close);
        Assert.assertEquals("Unexpected number of locks after all have been released.", 0, ks.getLockCount());
    }
}
