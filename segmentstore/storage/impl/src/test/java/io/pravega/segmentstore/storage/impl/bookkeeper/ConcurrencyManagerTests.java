/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.bookkeeper;

import io.pravega.common.AbstractTimer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the ConcurrencyManager class.
 */
public class ConcurrencyManagerTests {
    private static final int MIN_PARALLELISM = 1;
    private static final int MAX_PARALLELISM = 100;
    private static final long TIME_INCREMENT = ConcurrencyManager.UPDATE_PERIOD_MILLIS * AbstractTimer.NANOS_TO_MILLIS;

    /**
     * Tests the ability to update the parallelism in response to increased observed throughput.
     */
    @Test
    public void testIncreasingThroughput() {
        final int maxParallelism = 10; // Much smaller value, otherwise this test would never end.
        final int steps = maxParallelism + 1;
        final int writeSize = BookKeeperConfig.MAX_APPEND_LENGTH / 10;
        val time = new AtomicLong(0);
        val m = new ConcurrencyManager(MIN_PARALLELISM, maxParallelism, time::get);
        int previousParallelism = m.getCurrentParallelism();
        Assert.assertTrue("Parallelism out of bounds: " + previousParallelism,
                MIN_PARALLELISM <= previousParallelism && previousParallelism <= maxParallelism);
        long previousTotalWriteLength = 1;
        for (int i = 0; i < steps; i++) {
            // Record a bunch of writes, but make sure we always record significantly more than last time, otherwise no
            // change would happen.
            long totalWriteLength = 0;
            while (totalWriteLength < 1 + (1 + ConcurrencyManager.SIGNIFICANT_DIFFERENCE) * previousTotalWriteLength) {
                m.writeCompleted(writeSize);
                totalWriteLength += writeSize;
            }

            previousTotalWriteLength = totalWriteLength;
            time.addAndGet(TIME_INCREMENT);

            int newParallelism = m.getOrUpdateParallelism();
            int expectedParallelism = Math.min(previousParallelism + 1, maxParallelism);
            Assert.assertEquals("Unexpected new value of parallelism.", expectedParallelism, newParallelism);
            Assert.assertEquals("Unexpected value from getCurrentParallelism.", newParallelism, m.getCurrentParallelism());
            previousParallelism = newParallelism;
        }
    }

    /**
     * Tests the ability to update the parallelism in response to decreased observed throughput.
     */
    @Test
    public void testDecreasingThroughput() {
        final int steps = 5;
        final int writeSize = BookKeeperConfig.MAX_APPEND_LENGTH / 10;
        final long initialWriteLength = 10 * writeSize;
        val time = new AtomicLong(0);
        val m = create(time::get);

        // Initial setup - this will end up increasing the parallelism a bit, so we want to exclude it from our tests.
        long previousTotalWriteLength = 0;
        while (previousTotalWriteLength < initialWriteLength) {
            m.writeCompleted(writeSize);
            previousTotalWriteLength += writeSize;
        }
        time.addAndGet(TIME_INCREMENT);
        int previousParallelism = m.getOrUpdateParallelism();

        for (int i = 0; i < steps; i++) {
            // Record a bunch of writes, but make sure we always record significantly less than last time, otherwise no
            // change would happen.
            long totalWriteLength = 0;
            while (totalWriteLength + writeSize < (1 - ConcurrencyManager.SIGNIFICANT_DIFFERENCE) * previousTotalWriteLength) {
                m.writeCompleted(writeSize);
                totalWriteLength += writeSize;
            }

            previousTotalWriteLength = totalWriteLength;
            time.addAndGet(TIME_INCREMENT);

            int newParallelism = m.getOrUpdateParallelism();
            int expectedParallelism = Math.max(previousParallelism - 1, MIN_PARALLELISM);
            Assert.assertEquals("Unexpected new value of parallelism.", expectedParallelism, newParallelism);
            Assert.assertEquals("Unexpected value from getCurrentParallelism.", newParallelism, m.getCurrentParallelism());
            previousParallelism = newParallelism;
        }
    }

    /**
     * Tests the ability to update the parallelism in response to insignificant changes in throughput.
     */
    @Test
    public void testInsignificantChange() {
        final int steps = 5;
        final int originalWriteSize = BookKeeperConfig.MAX_APPEND_LENGTH / 2;
        val time = new AtomicLong(0);
        val m = create(time::get);

        // Initial setup - this will end up increasing the parallelism a bit, so we want to exclude it from our tests.
        int currentWriteSize = originalWriteSize;
        m.writeCompleted(originalWriteSize);
        time.addAndGet(TIME_INCREMENT);
        final int expectedParallelism = m.getOrUpdateParallelism();

        for (int i = 0; i < steps; i++) {
            // Record a bunch of writes, but make sure we always record significantly less than last time, otherwise no
            // change would happen.
            boolean decrease = i % 2 == 0;
            if (decrease) {
                currentWriteSize -= ConcurrencyManager.SIGNIFICANT_DIFFERENCE * currentWriteSize - 10;
            } else {
                currentWriteSize += ConcurrencyManager.SIGNIFICANT_DIFFERENCE * currentWriteSize - 10;
            }

            m.writeCompleted(currentWriteSize);
            time.addAndGet(TIME_INCREMENT);

            int newParallelism = m.getOrUpdateParallelism();
            Assert.assertEquals("Unexpected new value of parallelism when decrease = " + decrease, expectedParallelism, newParallelism);
            Assert.assertEquals("Unexpected value from getCurrentParallelism.", newParallelism, m.getCurrentParallelism());
        }
    }

    /**
     * Test the ability of the ConcurrencyManager to handle very frequent calls to update.
     */
    @Test
    public void testUpdateTooSoon() {
        final int steps = 5;
        final int writeSize = BookKeeperConfig.MAX_APPEND_LENGTH / 2;
        val time = new AtomicLong(0);
        val m = create(time::get);

        // Initial write, so we have something to compare against.
        m.writeCompleted(writeSize);
        time.addAndGet(TIME_INCREMENT);
        int expectedParallelism = m.getOrUpdateParallelism();
        for (int i = 0; i < steps; i++) {
            m.writeCompleted(writeSize);
            time.addAndGet(TIME_INCREMENT / steps - 1);
            int parallelism = m.getOrUpdateParallelism();
            Assert.assertEquals("Unexpected new value of parallelism when no change was expected.", expectedParallelism, parallelism);
            Assert.assertEquals("Unexpected value from getCurrentParallelism.", parallelism, m.getCurrentParallelism());
        }

        // Now do make sure those writes we accumulated didn't vanish.
        time.addAndGet(steps);
        int parallelism = m.getOrUpdateParallelism();
        expectedParallelism++;
        Assert.assertEquals("Unexpected new value of parallelism when a change was expected.", expectedParallelism, parallelism);
        Assert.assertEquals("Unexpected value from getCurrentParallelism.", parallelism, m.getCurrentParallelism());
    }

    /**
     * Test the ability of the ConcurrencyManager to handle stale data, as well as very frequent calls to update.
     */
    @Test
    public void testStale() {
        final int steps = 5;
        final int writeSize = BookKeeperConfig.MAX_APPEND_LENGTH / 2;
        val time = new AtomicLong(0);
        val m = create(time::get);
        final int expectedParallelism = m.getCurrentParallelism(); // For stale data, we expect parallelism to be reverted.

        // Initial write, so we have something to compare against.
        m.writeCompleted(writeSize);
        time.addAndGet(TIME_INCREMENT);
        m.getOrUpdateParallelism();
        for (int i = 0; i < steps; i++) {
            m.writeCompleted(writeSize);
        }

        // Now do make sure those writes we accumulated didn't vanish.
        time.addAndGet(ConcurrencyManager.STALE_MILLIS * AbstractTimer.NANOS_TO_MILLIS + 1);
        int parallelism = m.getOrUpdateParallelism();
        Assert.assertEquals("Unexpected new value of parallelism when data is stale.", expectedParallelism, parallelism);
        Assert.assertEquals("Unexpected value from getCurrentParallelism.", parallelism, m.getCurrentParallelism());
    }

    private ConcurrencyManager create(Supplier<Long> timeSupplier) {
        return new ConcurrencyManager(MIN_PARALLELISM, MAX_PARALLELISM, timeSupplier);
    }
}
