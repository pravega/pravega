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
import io.pravega.common.MathHelpers;
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
     * Tests the ability to update the parallelism in response to increased observed throughput, regardless of other
     * factors, such as Latency.
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
        long latency = 1;
        for (int i = 0; i < steps; i++) {
            // Record a bunch of writes, but make sure we always record significantly more than last time, otherwise no
            // change would happen.
            long totalWriteLength = 0;
            while (totalWriteLength < 1 + (1 + ConcurrencyManager.SIGNIFICANT_DIFFERENCE) * previousTotalWriteLength) {
                m.writeCompleted(writeSize, latency);
                totalWriteLength += writeSize;
                latency++; // Keep increasing latency to verify we don't care about this factor when throughput increases.
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
     * Tests the ability to update the parallelism in response to decreased observed throughput, given constant latency.
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
            m.writeCompleted(writeSize, 1);
            previousTotalWriteLength += writeSize;
        }
        time.addAndGet(TIME_INCREMENT);
        int previousParallelism = m.getOrUpdateParallelism();
        boolean previousDecrease = false;
        for (int i = 0; i < steps; i++) {
            // Record a bunch of writes, but make sure we always record significantly less than last time, otherwise no
            // change would happen.
            long totalWriteLength = 0;
            while (totalWriteLength + writeSize < (1 - ConcurrencyManager.SIGNIFICANT_DIFFERENCE) * previousTotalWriteLength) {
                m.writeCompleted(writeSize, 1);
                totalWriteLength += writeSize;
            }

            previousTotalWriteLength = totalWriteLength;
            time.addAndGet(TIME_INCREMENT);

            int newParallelism = m.getOrUpdateParallelism();
            int expectedParallelism = MathHelpers.minMax(previousParallelism + (previousDecrease ? 1 : -1), MIN_PARALLELISM, MAX_PARALLELISM);
            Assert.assertEquals("Unexpected new value of parallelism.", expectedParallelism, newParallelism);
            Assert.assertEquals("Unexpected value from getCurrentParallelism.", newParallelism, m.getCurrentParallelism());
            previousDecrease = newParallelism < previousParallelism;
            previousParallelism = newParallelism;
        }
    }

    /**
     * Tests the ability to update the parallelism in response to insignificant changes in throughput and/or latency.
     */
    @Test
    public void testSmallThroughputChange() {
        final int steps = 50;
        final int originalWriteSize = BookKeeperConfig.MAX_APPEND_LENGTH / 2;
        val time = new AtomicLong(0);
        val m = create(time::get);

        // Initial setup - this will end up increasing the parallelism a bit, so we want to exclude it from our tests.
        int currentWriteSize = originalWriteSize;
        int currentLatency = steps * 1000;
        m.writeCompleted(originalWriteSize, currentLatency);
        time.addAndGet(TIME_INCREMENT);
        int expectedParallelism = m.getOrUpdateParallelism();

        for (int i = 0; i < steps; i++) {
            // Determine whether to decrease or increase throughput (both directions would be insignificant).
            boolean decreaseThroughput = i % 2 == 0;
            int writeSizeDelta = (int) (ConcurrencyManager.SIGNIFICANT_DIFFERENCE * currentWriteSize - 10);
            currentWriteSize += decreaseThroughput ? -writeSizeDelta : writeSizeDelta;

            // Determine whether to make a significant latency change.
            boolean significantLatencyChange = i % 2 == 0;
            int latencyDelta = (int) (ConcurrencyManager.SIGNIFICANT_DIFFERENCE * currentLatency);
            latencyDelta += significantLatencyChange ? 10 : -10;

            // Determine whether to decrease or increase latency.
            boolean decreaseLatency = i % 4 >= 2;
            currentLatency += decreaseLatency ? -latencyDelta : latencyDelta;

            m.writeCompleted(currentWriteSize, currentLatency);
            time.addAndGet(TIME_INCREMENT);

            int newParallelism = m.getOrUpdateParallelism();
            if (significantLatencyChange) {
                expectedParallelism += decreaseLatency ? 1 : -1; // Changes in the opposite order to latency change.
            }

            String type = String.format("Tput %s, Latency %s %ssignificantly.",
                    decreaseThroughput ? "decreased" : "increased",
                    decreaseLatency ? "decreased" : "increased",
                    significantLatencyChange ? "" : "in");
            Assert.assertEquals("Unexpected new value of parallelism when " + type,
                    expectedParallelism, newParallelism);
            Assert.assertEquals("Unexpected value from getCurrentParallelism when " + type,
                    newParallelism, m.getCurrentParallelism());
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
        m.writeCompleted(writeSize, 1);
        time.addAndGet(TIME_INCREMENT);
        int expectedParallelism = m.getOrUpdateParallelism();
        for (int i = 0; i < steps; i++) {
            m.writeCompleted(writeSize, 1);
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
     * Test the ability of the ConcurrencyManager to handle stale data.
     */
    @Test
    public void testStale() {
        final int steps = 5;
        final int writeSize = BookKeeperConfig.MAX_APPEND_LENGTH / 2;
        val time = new AtomicLong(0);
        val m = create(time::get);
        final int expectedParallelism = m.getCurrentParallelism(); // For stale data, we expect parallelism to be reverted.

        // Initial write, so we have something to compare against.
        m.writeCompleted(writeSize, 1);
        time.addAndGet(TIME_INCREMENT);
        m.getOrUpdateParallelism();
        for (int i = 0; i < steps; i++) {
            m.writeCompleted(writeSize, 1);
        }

        // Now make sure that after a long time, all stats are reset and we revert back to the original parallelism.
        time.addAndGet(ConcurrencyManager.STALE_MILLIS * AbstractTimer.NANOS_TO_MILLIS + 1);
        int parallelism = m.getOrUpdateParallelism();
        Assert.assertEquals("Unexpected new value of parallelism when data is stale.", expectedParallelism, parallelism);
        Assert.assertEquals("Unexpected value from getCurrentParallelism.", parallelism, m.getCurrentParallelism());
    }

    /**
     * Tests the ability to nudge the degree of parallelism in either direction if it gets stuck at the same value for too
     * long.
     */
    @Test
    public void testStagnation() {
        final int maxParallelism = 5;
        final int steps = maxParallelism * ConcurrencyManager.MAX_STAGNATION_AGE * 2 + 1;
        final int writeSize = 12345;
        val time = new AtomicLong(0);
        val m = new ConcurrencyManager(MIN_PARALLELISM, maxParallelism, time::get);

        // Initial write, so we have something to compare against.
        m.writeCompleted(writeSize, 1);
        time.addAndGet(TIME_INCREMENT);
        int previousParallelism = m.getOrUpdateParallelism();
        int currentAge = 1;
        for (int i = 0; i < steps; i++) {
            m.writeCompleted(writeSize, 1);
            time.addAndGet(TIME_INCREMENT);

            int newParallelism = m.getOrUpdateParallelism();
            int expectedParallelism = previousParallelism;
            if (currentAge >= ConcurrencyManager.MAX_STAGNATION_AGE) {
                if (expectedParallelism == MIN_PARALLELISM) {
                    expectedParallelism++;
                } else {
                    expectedParallelism--;
                }
                currentAge = 1;
            } else {
                currentAge++;
            }

            Assert.assertEquals("Unexpected new value of parallelism.", expectedParallelism, newParallelism);
            Assert.assertEquals("Unexpected value from getCurrentParallelism.", newParallelism, m.getCurrentParallelism());
            previousParallelism = newParallelism;
        }

    }

    /**
     * Verifies that the parallelism stays constant if MinParallelism == MaxParallelism.
     */
    @Test
    public void testMinMaxEquals() {
        final int parallelism = 10;
        final int steps = parallelism + 1;
        final int writeSize = BookKeeperConfig.MAX_APPEND_LENGTH / 10;
        val time = new AtomicLong(0);
        val m = new ConcurrencyManager(parallelism, parallelism, time::get);
        Assert.assertEquals("Unexpected initial parallelism.", parallelism, m.getCurrentParallelism());
        long previousTotalWriteLength = 1;
        long latency = 1;
        for (int i = 0; i < steps; i++) {
            // Record a bunch of writes, but make sure we always record significantly more than last time, otherwise the
            // algorithm wouldn't have triggered anyway.
            long totalWriteLength = 0;
            while (totalWriteLength < 1 + (1 + ConcurrencyManager.SIGNIFICANT_DIFFERENCE) * previousTotalWriteLength) {
                m.writeCompleted(writeSize, latency);
                totalWriteLength += writeSize;
                latency++;
            }

            previousTotalWriteLength = totalWriteLength;
            time.addAndGet(TIME_INCREMENT);

            int newParallelism = m.getOrUpdateParallelism();
            Assert.assertEquals("Not expecting parallelism to change.", parallelism, newParallelism);
            Assert.assertEquals("Not expecting parallelism to change.", parallelism, m.getCurrentParallelism());
        }
    }

    private ConcurrencyManager create(Supplier<Long> timeSupplier) {
        return new ConcurrencyManager(MIN_PARALLELISM, MAX_PARALLELISM, timeSupplier);
    }
}
