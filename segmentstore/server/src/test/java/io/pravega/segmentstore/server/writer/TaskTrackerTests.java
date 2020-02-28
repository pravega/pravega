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

import io.pravega.segmentstore.server.ManualTimer;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.IntentionalException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link TaskTracker}.
 */
public class TaskTrackerTests {
    private static final int SHORT_TIMEOUT = 200;

    /**
     * Tests the {@link TaskTracker#getElapsedSinceCreation()} method.
     */
    @Test
    public void testGetElapsedSinceCreation() {
        val timer = new ManualTimer();
        timer.setElapsedMillis(1234);
        val t = new TaskTracker(timer);
        Assert.assertEquals(0, t.getElapsedSinceCreation().toMillis());
        timer.setElapsedMillis(1239);
        Assert.assertEquals(5, t.getElapsedSinceCreation().toMillis());
    }

    /**
     * Tests the {@link TaskTracker#track} method.
     */
    @Test
    public void testTrack() throws Exception {
        val timer = new ManualTimer();
        timer.setElapsedMillis(0);
        val tracker = new TaskTracker(timer);

        // Add one tracked item.
        val f1 = new CompletableFuture<Long>();
        val args1 = new Object[]{"a1", "a2"};
        val f1Result = tracker.track(() -> f1, args1);
        Assert.assertFalse("Not expecting result future to be complete yet.", f1.isDone());
        timer.setElapsedMillis(10);
        Assert.assertEquals(10, tracker.getLongestPendingDuration().toMillis());
        val pending1 = getPending(tracker);
        Assert.assertEquals(1, pending1.size());
        checkPending(10, args1, pending1.get(0));

        // Add a second one.
        val f2 = new CompletableFuture<Long>();
        val f2Result = tracker.track(() -> f2);
        timer.setElapsedMillis(20);
        Assert.assertEquals(20, tracker.getLongestPendingDuration().toMillis());
        val pending2 = getPending(tracker);
        Assert.assertEquals(2, pending2.size());
        checkPending(20, args1, pending2.get(0));
        checkPending(10, null, pending2.get(1));

        // Add a third.
        val f3 = new CompletableFuture<Long>();
        val args3 = new Object[0];
        val f3Result = tracker.track(() -> f3, args3);
        timer.setElapsedMillis(30);

        // The fourth one will fail synchronously.
        AssertExtensions.assertSuppliedFutureThrows(
                "Exception did not bubble up.",
                () -> tracker.track(() -> {
                    throw new IntentionalException();
                }), ex -> ex instanceof IntentionalException);
        Assert.assertEquals(30, tracker.getLongestPendingDuration().toMillis());
        val pending3 = getPending(tracker);
        Assert.assertEquals(3, pending3.size());
        checkPending(30, args1, pending3.get(0));
        checkPending(20, null, pending3.get(1));
        checkPending(10, args3, pending3.get(2));

        // Complete the second one.
        timer.setElapsedMillis(40);
        f2.complete(1L);
        Assert.assertEquals(1L, (long) f2Result.get(SHORT_TIMEOUT, TimeUnit.MILLISECONDS));
        Assert.assertEquals(40, tracker.getLongestPendingDuration().toMillis()); // f1 hasn't completed yet.
        val pending4 = getPending(tracker);
        Assert.assertEquals(2, pending4.size());
        checkPending(40, args1, pending4.get(0));
        checkPending(20, args3, pending4.get(1));

        // Fail the first one.
        timer.setElapsedMillis(50);
        f1.completeExceptionally(new IntentionalException());
        AssertExtensions.assertSuppliedFutureThrows(
                "Async exception did not propagate.",
                () -> f1Result,
                ex -> ex instanceof IntentionalException);
        Assert.assertEquals(30, tracker.getLongestPendingDuration().toMillis()); // f3 is the only one remaining.
        val pending5 = getPending(tracker);
        Assert.assertEquals(1, pending5.size());
        checkPending(30, args3, pending5.get(0));

        // Complete the third one.
        f3.complete(123456L);
        Assert.assertEquals(123456L, (long) f3Result.get(SHORT_TIMEOUT, TimeUnit.MILLISECONDS));
        Assert.assertEquals(0, tracker.getLongestPendingDuration().toMillis()); // Nothing pending.
        Assert.assertEquals(0, tracker.getAllPending().size());
    }

    private void checkPending(long millis, Object[] args, TaskTracker.PendingTask actual) {
        Assert.assertEquals("Unexpected elapsed time for " + actual, millis, actual.getElapsed().toMillis());
        if (args == null || args.length == 0) {
            Assert.assertEquals("Not expecting any args for " + actual, 0, actual.getContext().length);
        } else {
            Assert.assertSame("Unexpected args for " + actual, args, actual.getContext());
        }
    }

    private List<TaskTracker.PendingTask> getPending(TaskTracker tracker) {
        return tracker.getAllPending().stream()
                .sorted((p1, p2) -> Long.compare(p2.getElapsed().toMillis(), p1.getElapsed().toMillis()))
                .collect(Collectors.toList());
    }
}
