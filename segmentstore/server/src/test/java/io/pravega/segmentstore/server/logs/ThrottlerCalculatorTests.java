/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.logs;

import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.test.common.AssertExtensions;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the ThrottlerCalculator class.
 */
public class ThrottlerCalculatorTests {

    /**
     * Tests the ability to properly calculate throttling delays caused by cache overflows.
     */
    @Test
    public void testCacheThrottling() {
        val t = 0.85;
        val tAdj = t + ThrottlerCalculator.CACHE_TARGET_UTILIZATION_THRESHOLD_ADJUSTMENT;
        val maxU = 0.98;
        val cacheUtilization = new AtomicReference<Double>(0.0);
        val tc = ThrottlerCalculator.builder().cacheThrottler(cacheUtilization::get, t, maxU).build();
        testThrottling(tc, cacheUtilization,
                new Double[]{-1.0, 0.0, 0.5, tAdj},
                new Double[]{tAdj + 0.01, tAdj + 0.05, tAdj + 0.06, maxU},
                new Double[]{maxU, maxU + 0.01, maxU * 2, Double.MAX_VALUE});

        // Now verify behavior when the max threshold is less than the min threshold.
        val tc2 = ThrottlerCalculator.builder().cacheThrottler(cacheUtilization::get, t, t - 0.01).build();
        testThrottling(tc2, cacheUtilization,
                new Double[]{-1.0, 0.0, 0.5, tAdj},
                new Double[0],
                new Double[]{tAdj + 0.01, tAdj + 0.05, tAdj + 0.06, maxU, maxU + 0.01, maxU * 2, Double.MAX_VALUE});
    }

    /**
     * Tests the ability to properly calculate throttling delays caused by commit log queue overflows.
     */
    @Test
    public void testCommitBacklogThrottling() {
        val maxU = ThrottlerCalculator.COMMIT_BACKLOG_COUNT_FULL_THROTTLE_THRESHOLD;
        val commitLogCount = new AtomicReference<Integer>(0);
        val tc = ThrottlerCalculator.builder().commitBacklogThrottler(commitLogCount::get).build();
        testThrottling(tc, commitLogCount,
                new Integer[]{-1, 0, ThrottlerCalculator.COMMIT_BACKLOG_COUNT_THRESHOLD / 2, ThrottlerCalculator.COMMIT_BACKLOG_COUNT_THRESHOLD},
                new Integer[]{ThrottlerCalculator.COMMIT_BACKLOG_COUNT_THRESHOLD + 1, ThrottlerCalculator.COMMIT_BACKLOG_COUNT_THRESHOLD + 10, maxU},
                new Integer[]{maxU, maxU + 1, maxU * 2, 10000});
    }

    /**
     * Tests the ability to properly calculate batching-related delays.
     */
    @Test
    public void testBatching() {
        val increment = 0.1;
        val queueStats = new AtomicReference<QueueStats>(null);
        val tc = ThrottlerCalculator.builder().batchingThrottler(queueStats::get).build();

        // Test variance based on Fill Ratio (uncapped).
        // Set the initial lastValue to the max, so we verify that we won't exceed this value.
        AtomicInteger lastValue = new AtomicInteger(ThrottlerCalculator.MAX_BATCHING_DELAY_MILLIS);
        for (double fillRatio = 0.0; fillRatio <= 1.0; fillRatio += increment) {
            queueStats.set(new QueueStats(100, fillRatio, ThrottlerCalculator.MAX_BATCHING_DELAY_MILLIS));
            val value = tc.getThrottlingDelay().getDurationMillis();
            if (fillRatio < increment / 2) {
                // This is essentially 0.0, but it's hard to compare precisely against double.
                Assert.assertEquals("Expected maximum batching when fill ratio is 0.", lastValue.get(), value);
            } else {
                if (fillRatio > 1.0 - increment / 2) {
                    Assert.assertEquals("Expected maximum batching when fill ratio is 1.0.", 0, value);
                }

                AssertExtensions.assertLessThan("Expecting batching delay to decrease as fill ratio increases: " + fillRatio,
                        lastValue.get(), value);
            }

            lastValue.set(value);
        }

        // Test capping at max.
        Arrays.stream(new QueueStats[]{
                new QueueStats(100, 0.0, ThrottlerCalculator.MAX_BATCHING_DELAY_MILLIS + 1),
                new QueueStats(100, 0.5, ThrottlerCalculator.MAX_BATCHING_DELAY_MILLIS * 10),
                new QueueStats(100, 0.9, ThrottlerCalculator.MAX_BATCHING_DELAY_MILLIS * 100)})
              .forEach(qs -> {
                  queueStats.set(qs);
                  Assert.assertEquals("Expected batching to be capped.", ThrottlerCalculator.MAX_BATCHING_DELAY_MILLIS, tc.getThrottlingDelay().getDurationMillis());
              });
    }


    private <T extends Comparable<T>> void testThrottling(ThrottlerCalculator tc, AtomicReference<T> inputValue, T[] noThrottleValues, T[] gradualThrottleValues, T[] maxThrottleValues) {
        // Test for values where we don't expect throttling.
        Arrays.stream(noThrottleValues)
                .forEach(v -> {
                    inputValue.set(v);
                    Assert.assertFalse("Unexpected value from isThrottlingRequired() when no throttling expected: " + v,
                            tc.isThrottlingRequired());
                    ThrottlerCalculator.DelayResult r = tc.getThrottlingDelay();
                    Assert.assertEquals("Unexpected value from getDurationMillis() when no throttling expected " + v, 0, r.getDurationMillis());
                    Assert.assertFalse("Unexpected value from isMaximum() when no throttling expected " + v, r.isMaximum());
                });

        // Test for values where we expect gradual throttling, up to max.
        AtomicInteger lastValue = new AtomicInteger();
        Arrays.stream(gradualThrottleValues)
                .forEach(v -> {
                    // For this test, we want our test values to be pre-sorted.
                    Assert.assertTrue("Test setup failure: non-increasing test value.", Comparator.<T>naturalOrder().compare(inputValue.get(), v) < 0);
                    inputValue.set(v);
                    Assert.assertTrue("Unexpected value from isThrottlingRequired() when throttling is expected: " + v,
                            tc.isThrottlingRequired());
                    ThrottlerCalculator.DelayResult r = tc.getThrottlingDelay();
                    AssertExtensions.assertGreaterThan("Expected throttling value to increase when test value increases: " + v,
                            lastValue.get(), r.getDurationMillis());
                    AssertExtensions.assertLessThanOrEqual("Expected throttling value to be capped: " + v,
                            ThrottlerCalculator.MAX_DELAY_MILLIS, r.getDurationMillis());
                    Assert.assertEquals("Unexpected value from isMaximum() " + v, r.getDurationMillis() >= ThrottlerCalculator.MAX_DELAY_MILLIS, r.isMaximum());
                    lastValue.set(r.getDurationMillis());
                });

        // Test for values that exceed max cache utilization.
        Arrays.stream(maxThrottleValues)
                .forEach(v -> {
                    inputValue.set(v);
                    Assert.assertTrue("Unexpected value from isThrottlingRequired() when max throttling is expected: " + v,
                            tc.isThrottlingRequired());
                    ThrottlerCalculator.DelayResult r = tc.getThrottlingDelay();
                    Assert.assertEquals("Unexpected value from getDurationMillis() when max throttling is expected " + v,
                            ThrottlerCalculator.MAX_DELAY_MILLIS, r.getDurationMillis());
                    Assert.assertTrue("Unexpected value from isMaximum() when maximum throttling expected " + v, r.isMaximum());
                });
    }
}
