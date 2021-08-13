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

import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
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
    private static final int TIMEOUT_MILLIS = 30 * 1000;
    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

    /**
     * Tests various method with invalid arguments.
     */
    @Test
    public void testInvalidArguments() {
        final int credits = 10;
        AssertExtensions.assertThrows(
                "constructor: totalCredits < 0",
                () -> new AsyncSemaphore(-1, 0, ""),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "constructor: totalCredits == 0",
                () -> new AsyncSemaphore(0, 0, ""),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "constructor: usedCredits < 0",
                () -> new AsyncSemaphore(1, -1, ""),
                ex -> ex instanceof IllegalArgumentException);
        @Cleanup
        val s = new AsyncSemaphore(credits, 0, "");
        AssertExtensions.assertThrows(
                "release: credits < 0",
                () -> s.release(-1),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "run: credits < 0",
                () -> s.run(CompletableFuture::new, -1, false),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "run: credits > totalCredits",
                () -> s.run(CompletableFuture::new, credits + 1, false),
                ex -> ex instanceof IllegalArgumentException);
        Assert.assertEquals("Not expecting any queued tasks.", 0, s.getQueueSize());
        Assert.assertEquals("Not expecting used credits.", 0, s.getUsedCredits());
    }

    /**
     * Tests the {@link AsyncSemaphore#run} and {@link AsyncSemaphore#release} methods when tasks complete without
     * exceptions.
     */
    @Test
    public void testAcquireRelease() throws Exception {
        final int credits = 100;
        final int initialUsedCredits = credits / 5;
        final int immediateTaskCount = credits - initialUsedCredits;
        final int queuedTaskCount = credits / 2;

        @Cleanup
        val s = new AsyncSemaphore(credits, initialUsedCredits, "");
        Assert.assertEquals("Unexpected initial used credits.", initialUsedCredits, s.getUsedCredits());

        val tasks = new HashMap<CompletableFuture<Integer>, CompletableFuture<Integer>>();

        // 1. We add a number of tasks that should not be queued (i.e., executed immediately).
        for (int i = 0; i < immediateTaskCount; i++) {
            CompletableFuture<Integer> task = new CompletableFuture<>();
            val result = s.run(() -> task, 1, false);
            tasks.put(task, result);
            Assert.assertEquals("For immediate execution, expecting the same Future to be returned.", task, result);
        }

        Assert.assertEquals("Unexpected used credits before queuing.", credits, s.getUsedCredits());

        // 1.1. Release some resources and add more non-queued tasks.
        final int toRelease = initialUsedCredits / 2;
        s.release(toRelease);
        Assert.assertEquals("Unexpected used credits.", credits - toRelease, s.getUsedCredits());
        for (int i = 0; i < toRelease; i++) {
            CompletableFuture<Integer> task = new CompletableFuture<>();
            val result = s.run(() -> task, 1, false);
            tasks.put(task, result);
            Assert.assertEquals("For immediate execution, expecting the same Future to be returned.", task, result);
        }

        Assert.assertEquals("Unexpected used credits before queueing.", credits, s.getUsedCredits());
        Assert.assertEquals("Not expecting any queued items yet.", 0, s.getQueueSize());

        // Complete immediate tasks.
        tasks.keySet().forEach(f -> f.complete(-1));
        tasks.clear();

        // 2. Add a number of tasks that should be queued.
        for (int i = 0; i < queuedTaskCount; i++) {
            CompletableFuture<Integer> task = new CompletableFuture<>();
            val result = s.run(() -> task, 1, false);
            tasks.put(task, result);
            Assert.assertNotEquals("For delayed execution, expecting the different Future to be returned.", task, result);
        }

        Assert.assertEquals("Unexpected used credits after queuing.", credits, s.getUsedCredits());
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
                val result = s.run(() -> task, 1, false);
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

        Assert.assertEquals("Unexpected final used credits.", credits, s.getUsedCredits());
    }

    /**
     * Tests the {@link AsyncSemaphore#run} and {@link AsyncSemaphore#release} methods when the {@code force} flag is
     * set.
     */
    @Test
    public void testAcquireReleaseForce() {
        final int maxCredits = 100;
        final int immediateTaskCount = maxCredits;
        final int forcedTaskCount = maxCredits / 2;
        final int queuedTaskCount = maxCredits;

        @Cleanup
        val s = new AsyncSemaphore(maxCredits, 0, "");
        val tasks = new ArrayList<Map.Entry<CompletableFuture<Integer>, CompletableFuture<Integer>>>();

        // We will collect the actual execution order of these tasks to verify they are invoked in the proper order.
        val nextTaskId = new AtomicInteger(0);
        val executionOrder = Collections.synchronizedList(new ArrayList<Integer>());

        BiConsumer<Boolean, Boolean> addNewTask = (force, expectQueued) -> {
            CompletableFuture<Integer> task = new CompletableFuture<>();
            val taskId = nextTaskId.getAndIncrement();
            val result = s.run(() -> {
                executionOrder.add(taskId);
                return task;
            }, 1, force);
            tasks.add(new AbstractMap.SimpleImmutableEntry<>(task, result));
            if (expectQueued) {
                Assert.assertNotEquals("For delayed execution, expecting the different Future to be returned.", task, result);
            } else {
                Assert.assertEquals("For immediate execution, expecting the same Future to be returned.", task, result);
            }
        };

        // 1. Add a number of tasks that should not be queued (i.e., executed immediately).
        for (int i = 0; i < immediateTaskCount; i++) {
            addNewTask.accept(i % 2 == 0, false);
        }

        // 1.1. Add a number of forced tasks. These should also be executed immediately.
        Assert.assertEquals("Unexpected used credits before forcing.", maxCredits, s.getUsedCredits());
        for (int i = 0; i < forcedTaskCount; i++) {
            addNewTask.accept(true, false);
        }

        // 1.2. Add queued tasks.
        int expectedCredits = maxCredits + forcedTaskCount;
        Assert.assertEquals("Unexpected used credits before queuing.", maxCredits + forcedTaskCount, s.getUsedCredits());
        for (int i = 0; i < queuedTaskCount; i++) {
            addNewTask.accept(false, true);
        }

        Assert.assertEquals("Unexpected number of items queued.", queuedTaskCount, s.getQueueSize());

        // Complete immediate tasks.
        val taskIterator = tasks.iterator();
        for (int i = 0; i < immediateTaskCount; i++) {
            taskIterator.next().getKey().complete(-1);
            s.release(1);

            // The number of used credits should gradually decrease (as it currently overflows due to the forced tasks),
            // but it should not drop below the max allowed credits since we have a big queue of pending tasks.
            expectedCredits = Math.max(maxCredits, expectedCredits - 1);
            Assert.assertEquals("Unexpected used credits after releasing initial task " + i, expectedCredits, s.getUsedCredits());
        }

        val expectedQueuedTaskCount = queuedTaskCount - forcedTaskCount;
        Assert.assertEquals("Expected items queued up before releasing forced tasks.", expectedQueuedTaskCount, s.getQueueSize());

        // Complete forced tasks.
        for (int i = 0; i < forcedTaskCount; i++) {
            taskIterator.next().getKey().complete(-1);
            s.release(1);

            expectedCredits = Math.max(maxCredits, expectedCredits - 1);
            Assert.assertEquals("Unexpected used credits after releasing forced task " + i, expectedCredits, s.getUsedCredits());
        }

        Assert.assertEquals("Not expecting any more items queued after releasing all forced tasks.", 0, s.getQueueSize());

        // Complete remaining tasks.
        for (int i = 0; i < queuedTaskCount; i++) {
            taskIterator.next().getKey().complete(-1);
            s.release(1);

            expectedCredits--;
            Assert.assertEquals("Unexpected used credits after releasing queued task " + i, expectedCredits, s.getUsedCredits());
        }

        Assert.assertFalse("Not expecting any more tasks to process.", taskIterator.hasNext());

        // Check execution order.
        int totalTaskCount = immediateTaskCount + forcedTaskCount + queuedTaskCount;
        Assert.assertEquals("Unexpected number of tasks executed.", totalTaskCount, executionOrder.size());
        for (int i = 0; i < totalTaskCount; i++) {
            Assert.assertEquals("Tasks not executed in proper order at index " + i, i, (int) executionOrder.get(i));
        }
    }

    /**
     * Tests the {@link AsyncSemaphore#run} and {@link AsyncSemaphore#release} methods when tasks are executed immediately
     * but complete with exceptions
     */
    @Test
    public void testFailedImmediateTasks() {
        testFailedTasks(10, 10);
    }

    /**
     * Tests the {@link AsyncSemaphore#run} and {@link AsyncSemaphore#release} methods when tasks are delayed
     * but complete with exceptions
     */
    @Test
    public void testFailedDelayedTasks() {
        testFailedTasks(10, 100);
    }

    private void testFailedTasks(int credits, int toAdd) {
        @Cleanup
        val s = new AsyncSemaphore(credits, 0, "");
        val tasks = new HashMap<CompletableFuture<Integer>, CompletableFuture<Integer>>();

        // 1. We add a number of tasks that should not be queued (i.e., executed immediately).
        int expectedUsedCredits = 0;
        for (int i = 0; i < toAdd; i++) {
            CompletableFuture<Integer> task = new CompletableFuture<>();
            boolean failSync = i % 2 == 0;
            val result = s.run(() -> {
                if (failSync) {
                    throw new IntentionalException();
                } else {
                    return task;
                }
            }, 1, i % 2 == 0);
            if (!failSync) {
                tasks.put(task, result);
                expectedUsedCredits = Math.min(credits, expectedUsedCredits + 1);
            }
        }

        Assert.assertEquals("Unexpected used credits before async failing.", expectedUsedCredits, s.getUsedCredits());

        // 1.1. Fail all of those tasks.
        tasks.keySet().forEach(f -> f.completeExceptionally(new IntentionalException()));
        AssertExtensions.assertThrows(
                "Expecting exception to have been propagated.",
                () -> Futures.allOf(tasks.values()).get(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS),
                ex -> ex instanceof IntentionalException);

        Assert.assertEquals("Unexpected used credits after async failing.", 0, s.getUsedCredits());
    }

    /**
     * Tests the {@link AsyncSemaphore#close()} method and its ability to cancel queued tasks.
     */
    @Test
    public void testClose() {
        @Cleanup
        val s = new AsyncSemaphore(1, 1, "");
        val tasks = new HashMap<CompletableFuture<Integer>, CompletableFuture<Integer>>();
        for (int i = 0; i < 5; i++) {
            CompletableFuture<Integer> task = new CompletableFuture<>();
            val result = s.run(() -> task, 1, false);
            tasks.put(task, result);
        }

        s.close();
        Assert.assertTrue("Expecting all queued tasks to have been cancelled.",
                tasks.values().stream().allMatch(CompletableFuture::isCancelled));
        Assert.assertEquals("Unexpected final used credits.", 0, s.getUsedCredits());
    }
}
