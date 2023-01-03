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
package io.pravega.segmentstore.server.logs;

import io.pravega.segmentstore.storage.QueueStats;
import io.pravega.segmentstore.storage.WriteSettings;
import io.pravega.test.common.AssertExtensions;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the ThrottlerCalculator class.
 */
public class ThrottlerCalculatorTests {
    private static final int MAX_APPEND_LENGTH = 1024 * 1024 - 1024;
    /**
     * Tests the ability to properly calculate throttling delays caused by cache overflows.
     */
    @Test
    public void testCacheThrottling() {
        val t = 0.85;
        val tAdj = t + ThrottlerPolicy.CACHE_TARGET_UTILIZATION_THRESHOLD_ADJUSTMENT;
        val maxU = 0.98;
        val cacheUtilization = new AtomicReference<Double>(0.0);
        val tc = ThrottlerCalculator.builder()
                .maxDelayMillis(DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue())
                .cacheThrottler(cacheUtilization::get, t, maxU, DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue())
                .build();
        testThrottling(tc, cacheUtilization,
                new Double[]{-1.0, 0.0, 0.5, tAdj},
                new Double[]{tAdj + 0.01, tAdj + 0.05, tAdj + 0.06, maxU},
                new Double[]{maxU, maxU + 0.01, maxU * 2, Double.MAX_VALUE});

        // Now verify behavior when the max threshold is less than the min threshold.
        val tc2 = ThrottlerCalculator.builder()
                .maxDelayMillis(DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue())
                .cacheThrottler(cacheUtilization::get, t, t - 0.01, DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue())
                .build();
        testThrottling(tc2, cacheUtilization,
                new Double[]{-1.0, 0.0, 0.5, tAdj},
                new Double[0],
                new Double[]{tAdj + 0.01, tAdj + 0.05, tAdj + 0.06, maxU, maxU + 0.01, maxU * 2, Double.MAX_VALUE});
    }

    /**
     * Tests the ability to properly calculate batching-related delays.
     */
    @Test
    public void testBatching() {
        val increment = 0.1;
        val queueSize = 100;
        val queueStats = new AtomicReference<QueueStats>(null);
        val tc = ThrottlerCalculator.builder()
                .maxDelayMillis(DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue())
                .batchingThrottler(queueStats::get, DurableLogConfig.MAX_BATCHING_DELAY_MILLIS.getDefaultValue())
                .build();

        // Test variance based on Fill Ratio (uncapped).
        // Set the initial lastValue to the max, so we verify that we won't exceed this value.
        AtomicInteger lastValue = new AtomicInteger(DurableLogConfig.MAX_BATCHING_DELAY_MILLIS.getDefaultValue());
        for (double fillRatio = 0.0; fillRatio <= 1.0; fillRatio += increment) {
            queueStats.set(createStats(queueSize, fillRatio, DurableLogConfig.MAX_BATCHING_DELAY_MILLIS.getDefaultValue()));
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
                new QueueStats(100, 0, MAX_APPEND_LENGTH, DurableLogConfig.MAX_BATCHING_DELAY_MILLIS.getDefaultValue() + 1),
                new QueueStats(100, MAX_APPEND_LENGTH / 2, MAX_APPEND_LENGTH, DurableLogConfig.MAX_BATCHING_DELAY_MILLIS.getDefaultValue() * 10),
                new QueueStats(100, MAX_APPEND_LENGTH - 1, MAX_APPEND_LENGTH, DurableLogConfig.MAX_BATCHING_DELAY_MILLIS.getDefaultValue() * 100)})
              .forEach(qs -> {
                  queueStats.set(qs);
                  Assert.assertEquals("Expected batching to be capped.", (int) DurableLogConfig.MAX_BATCHING_DELAY_MILLIS.getDefaultValue(), tc.getThrottlingDelay().getDurationMillis());
              });
    }

    /**
     * Tests the ability to properly calculate DurableDataLog-related delays.
     */
    @Test
    public void testDurableDataLog() {
        val halfRatio = 0.5;
        val maxWriteSize = 12345;
        val maxQueueCount = 123;
        val maxOutstandingBytes = maxWriteSize * maxQueueCount;
        val minThrottleThreshold = (int) (maxOutstandingBytes * ThrottlerPolicy.DURABLE_DATALOG_THROTTLE_THRESHOLD_FRACTION / maxWriteSize);
        val maxThrottleThreshold = (int) ((double) maxOutstandingBytes / maxWriteSize);
        val writeSettings = new WriteSettings(maxWriteSize, Duration.ofMillis(1234), maxOutstandingBytes);
        val thresholdMillis = (int) (writeSettings.getMaxWriteTimeout().toMillis() * ThrottlerPolicy.DURABLE_DATALOG_THROTTLE_THRESHOLD_FRACTION);
        val queueStats = new AtomicReference<QueueStats>(null);
        val tc = ThrottlerCalculator.builder()
                .maxDelayMillis(DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue())
                .durableDataLogThrottler(writeSettings, queueStats::get, DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue())
                .build();
        val noThrottling = new QueueStats[]{
                createStats(1, halfRatio, thresholdMillis - 1),
                createStats(minThrottleThreshold + 1, 1.0, thresholdMillis),
                createStats(maxThrottleThreshold * 2, 1.0, thresholdMillis)};
        val gradualThrottling = new QueueStats[]{
                createStats((int) (minThrottleThreshold / halfRatio) + 2, halfRatio, thresholdMillis + 1),
                createStats((int) (minThrottleThreshold / halfRatio) + 10, halfRatio, thresholdMillis + 1),
                createStats((int) (maxOutstandingBytes / halfRatio), halfRatio, thresholdMillis + 1)};
        val maxThrottling = new QueueStats[]{
                createStats((int) (maxOutstandingBytes / halfRatio) + 1, halfRatio, thresholdMillis + 1),
                createStats((int) (maxOutstandingBytes / halfRatio) * 2, halfRatio, thresholdMillis + 1)};
        testThrottling(tc, queueStats, noThrottling, gradualThrottling, maxThrottling);
    }

    /**
     * Tests the ability to properly calculate throttling delays caused by OperationLog overflows.
     */
    @Test
    public void testOperationLog() {
        val maxDelayMillis = DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue();
        val targetSize = DurableLogConfig.OPERATION_LOG_TARGET_SIZE.getDefaultValue();
        val maxSize = DurableLogConfig.OPERATION_LOG_MAX_SIZE.getDefaultValue();
        val logSize = new AtomicReference<>(0);
        val tc = ThrottlerCalculator.builder()
                .maxDelayMillis(maxDelayMillis)
                .operationLogThrottler(logSize::get, maxDelayMillis, maxSize, targetSize)
                .build();
        testThrottling(tc, logSize,
                new Integer[]{-1, 0, targetSize / 2, targetSize},
                new Integer[]{targetSize + 10, (maxSize + targetSize) >>> 1, maxSize},
                new Integer[]{maxSize, maxSize + 10, maxSize * 2, Integer.MAX_VALUE});
    }

    private QueueStats createStats(int queueSize, double fillRatio, int expectedProcessingTimeMillis) {
        int totalLength = (int) (fillRatio * MAX_APPEND_LENGTH * queueSize);
        return new QueueStats(queueSize, totalLength, MAX_APPEND_LENGTH, expectedProcessingTimeMillis);
    }

    private <T> void testThrottling(ThrottlerCalculator tc, AtomicReference<T> inputValue, T[] noThrottleValues, T[] gradualThrottleValues, T[] maxThrottleValues) {
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
                    // For this test, we need our test values to be pre-sorted in ascending order of throttling size.
                    inputValue.set(v);
                    Assert.assertTrue("Unexpected value from isThrottlingRequired() when throttling is expected: " + v,
                            tc.isThrottlingRequired());
                    ThrottlerCalculator.DelayResult r = tc.getThrottlingDelay();
                    AssertExtensions.assertGreaterThan("Expected throttling value to increase when test value increases: " + v,
                            lastValue.get(), r.getDurationMillis());
                    AssertExtensions.assertLessThanOrEqual("Expected throttling value to be capped: " + v,
                            DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue(), r.getDurationMillis());
                    Assert.assertEquals("Unexpected value from isMaximum() " + v, r.getDurationMillis() >= DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue(), r.isMaximum());
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
                            (int) DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue(), r.getDurationMillis());
                    Assert.assertTrue("Unexpected value from isMaximum() when maximum throttling expected " + v, r.isMaximum());
                });
    }
}
