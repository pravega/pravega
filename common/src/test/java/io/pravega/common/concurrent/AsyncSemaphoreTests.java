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
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the {@link AsyncSemaphore} class.
 */
public class AsyncSemaphoreTests {
    private final int TIMEOUT_MILLIS = 30 * 1000;
    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    /**
     * Tests various method with invalid arguments.
     */
    @Test
    public void testInvalidArguments() {
        final int capacity = 10;
        AssertExtensions.assertThrows(
                "constructor: totalCapacity < 0",
                () -> new AsyncSemaphore(0, -1),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "constructor: totalCapacity == 0",
                () -> new AsyncSemaphore(0, 0),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "constructor: usedCapacity < 0",
                () -> new AsyncSemaphore(-1, 1),
                ex -> ex instanceof IllegalArgumentException);

        val s = new AsyncSemaphore(0, capacity);
        AssertExtensions.assertThrows(
                "release: requestedCapacity < 0",
                () -> s.release(-1),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "acquire: requestedCapacity < 0",
                () -> s.acquire(-1, CompletableFuture::new),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "acquire: requestedCapacity > totalCapacity",
                () -> s.acquire(capacity + 1, CompletableFuture::new),
                ex -> ex instanceof IllegalArgumentException);
        Assert.assertEquals("Not expecting any queued tasks.", 0, s.getQueueSize());
        Assert.assertEquals("Not expecting used capacity.", 0, s.getUsedCapacity());
    }

    /**
     * Tests the {@link AsyncSemaphore#acquire} and {@link AsyncSemaphore#release} methods when tasks complete without
     * exceptions.
     */
    @Test
    public void testAcquireRelease() throws Exception {
        final int capacity = 100;
        final int initialUsedCapacity = capacity / 5;
        final int immediateTaskCount = capacity - initialUsedCapacity;
        final int queuedTaskCount = capacity / 2;

        @Cleanup
        val s = new AsyncSemaphore(initialUsedCapacity, capacity);
        Assert.assertEquals("Unexpected initial used capacity.", initialUsedCapacity, s.getUsedCapacity());

        val tasks = new HashMap<CompletableFuture<Integer>, CompletableFuture<Integer>>();

        // 1. We add a number of tasks that should not be queued (i.e., executed immediately).
        for (int i = 0; i < immediateTaskCount; i++) {
            CompletableFuture<Integer> task = new CompletableFuture<>();
            val result = s.acquire(1, () -> task);
            tasks.put(task, result);
            Assert.assertEquals("For immediate execution, expecting the same Future to be returned.", task, result);
        }

        Assert.assertEquals("Unexpected used capacity before queuing.", capacity, s.getUsedCapacity());

        // 1.1. Release some resources and add more non-queued tasks.
        final int toRelease = initialUsedCapacity / 2;
        s.release(toRelease);
        Assert.assertEquals("Unexpected used capacity.", capacity - toRelease, s.getUsedCapacity());
        for (int i = 0; i < toRelease; i++) {
            CompletableFuture<Integer> task = new CompletableFuture<>();
            val result = s.acquire(1, () -> task);
            tasks.put(task, result);
            Assert.assertEquals("For immediate execution, expecting the same Future to be returned.", task, result);
        }

        Assert.assertEquals("Unexpected used capacity before queueing.", capacity, s.getUsedCapacity());
        Assert.assertEquals("Not expecting any queued items yet.", 0, s.getQueueSize());

        // Complete immediate tasks.
        tasks.keySet().forEach(f -> f.complete(-1));
        tasks.clear();

        // 2. Add a number of tasks that should be queued.
        for (int i = 0; i < queuedTaskCount; i++) {
            CompletableFuture<Integer> task = new CompletableFuture<>();
            val result = s.acquire(1, () -> task);
            tasks.put(task, result);
            Assert.assertNotEquals("For delayed execution, expecting the different Future to be returned.", task, result);
        }

        Assert.assertEquals("Unexpected used capacity after queuing.", capacity, s.getUsedCapacity());
        Assert.assertEquals("Expected items to be queued up.", queuedTaskCount, s.getQueueSize());
        Assert.assertTrue("Not expecting any queued tasks to be completed yet.",
                tasks.values().stream().noneMatch(CompletableFuture::isDone));

        final int toReleaseQueuedAtOnce = queuedTaskCount / 10;
        int unreleasedTaskCount = tasks.size();
        while (unreleasedTaskCount > 0) {
            // Release a number of items.
            int r = Math.min(toReleaseQueuedAtOnce, unreleasedTaskCount);
            s.release(r);
            unreleasedTaskCount -= r;

            if (unreleasedTaskCount > 0) {
                // Add one more. Depending on circumstance, this may or may not be immediately executed.
                CompletableFuture<Integer> task = new CompletableFuture<>();
                val result = s.acquire(1, () -> task);
                tasks.put(task, result);
                unreleasedTaskCount++;
            }
        }

        // Complete all tasks and await their result's completion as well.
        val completeResult = new AtomicInteger(1);
        tasks.keySet().forEach(f -> f.complete(completeResult.getAndIncrement()));
        Futures.allOf(tasks.values()).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        // Verify that the results were properly propagated.
        for (val e : tasks.entrySet()) {
            val expected = e.getKey().join();
            val actual = e.getValue().join();
            Assert.assertEquals("Unexpected result.", expected, actual);
        }

        Assert.assertEquals("Unexpected final used capacity.", capacity, s.getUsedCapacity());
    }

    /**
     * Tests the {@link AsyncSemaphore#acquire} and {@link AsyncSemaphore#release} methods when tasks are executed immediately
     * but complete with exceptions
     */
    @Test
    public void testFailedImmediateTasks() {
        testFailedTasks(10, 10);
    }

    /**
     * Tests the {@link AsyncSemaphore#acquire} and {@link AsyncSemaphore#release} methods when tasks are delayed
     * but complete with exceptions
     */
    @Test
    public void testFailedDelayedTasks() {
        testFailedTasks(10, 100);
    }

    private void testFailedTasks(int capacity, int toAdd) {

        @Cleanup
        val s = new AsyncSemaphore(0, capacity);
        val tasks = new HashMap<CompletableFuture<Integer>, CompletableFuture<Integer>>();

        // 1. We add a number of tasks that should not be queued (i.e., executed immediately).
        int expectedUsedCapacity = 0;
        for (int i = 0; i < toAdd; i++) {
            CompletableFuture<Integer> task = new CompletableFuture<>();
            boolean failSync = i % 2 == 0;
            val result = s.acquire(1, () -> {
                if (failSync) {
                    throw new IntentionalException();
                } else {
                    return task;
                }
            });
            if (!failSync) {
                tasks.put(task, result);
                expectedUsedCapacity = Math.min(capacity, expectedUsedCapacity + 1);
            }
        }

        Assert.assertEquals("Unexpected used capacity before async failing.", expectedUsedCapacity, s.getUsedCapacity());

        // 1.1. Fail all of those tasks.
        tasks.keySet().forEach(f -> f.completeExceptionally(new IntentionalException()));
        AssertExtensions.assertThrows(
                "Expecting exception to have been propagated.",
                () -> Futures.allOf(tasks.values()).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                ex -> ex instanceof IntentionalException);

        Assert.assertEquals("Unexpected used capacity after async failing.", 0, s.getUsedCapacity());
    }

    /**
     * Tests the {@link AsyncSemaphore#close()} method and its ability to cancel queued tasks.
     */
    @Test
    public void testClose() {
        @Cleanup
        val s = new AsyncSemaphore(1, 1);
        val tasks = new HashMap<CompletableFuture<Integer>, CompletableFuture<Integer>>();
        for (int i = 0; i < 5; i++) {
            CompletableFuture<Integer> task = new CompletableFuture<>();
            val result = s.acquire(1, () -> task);
            tasks.put(task, result);
        }

        s.close();
        Assert.assertTrue("Expecting all queued tasks to have been cancelled.",
                tasks.values().stream().allMatch(CompletableFuture::isCancelled));
        Assert.assertEquals("Unexpected final used capacity.", 0, s.getUsedCapacity());
    }
}
