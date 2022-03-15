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

import io.pravega.common.util.ConfigurationException;
import io.pravega.segmentstore.server.SegmentStoreMetrics;
import io.pravega.shared.MetricsNames;
import io.pravega.shared.MetricsTags;
import io.pravega.shared.metrics.MetricRegistryUtils;
import io.pravega.shared.metrics.MetricsConfig;
import io.pravega.shared.metrics.MetricsProvider;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.SerializedClassRunner;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Cleanup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Unit tests for the {@link Throttler} class.
 */
@RunWith(SerializedClassRunner.class)
public class ThrottlerTests extends ThreadPooledTestSuite {
    private static final ThrottlerCalculator.ThrottlerName THROTTLER_NAME = ThrottlerCalculator.ThrottlerName.Cache;
    private static final int MAX_THROTTLE_MILLIS = DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue();
    private static final int NON_MAX_THROTTLE_MILLIS = MAX_THROTTLE_MILLIS - 1;
    private static final int TIMEOUT_MILLIS = 10000;
    private static final int SHORT_TIMEOUT_MILLIS = 50;
    private SegmentStoreMetrics.OperationProcessor metrics;
    private int containerId = 1;

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }

    @Before
    public void setUp() {
        this.containerId = new Random().nextInt(Integer.MAX_VALUE);
        MetricsProvider.initialize(MetricsConfig.builder()
                                                .with(MetricsConfig.ENABLE_STATISTICS, true)
                                                .build());
        MetricsProvider.getMetricsProvider().startWithoutExporting();
        this.metrics = new SegmentStoreMetrics.OperationProcessor(this.containerId);
    }

    @After
    public void tearDown() {
        this.metrics.close();
    }

    /**
     * Tests the {@link Throttler#isThrottlingRequired()} method.
     */
    @Test
    public void testThrottlingRequired() {
        val delays = Collections.<Integer>synchronizedList(new ArrayList<>());
        val calculator = new TestCalculatorThrottler(THROTTLER_NAME);
        @Cleanup
        Throttler t = new AutoCompleteTestThrottler(this.containerId, wrap(calculator), executorService(), metrics, delays::add);

        calculator.setThrottlingRequired(false);
        Assert.assertFalse("Not expecting any throttling to be required.", t.isThrottlingRequired());
        calculator.setThrottlingRequired(true);
        Assert.assertTrue("Expected throttling to be required.", t.isThrottlingRequired());

        Assert.assertFalse("Unexpected value from isClosed() before closing.", t.isClosed());
        t.close();
        Assert.assertTrue("Unexpected value from isClosed() after closing.", t.isClosed());
    }

    /**
     * Tests the case when {@link ThrottlerCalculator#getThrottlingDelay()} returns a value which does not warrant repeated
     * delays (i.e., not maximum delay)
     *
     * @throws Exception
     */
    @Test
    public void testSingleDelay() throws Exception {
        val delays = Collections.<Integer>synchronizedList(new ArrayList<>());
        val calculator = new TestCalculatorThrottler(THROTTLER_NAME);
        @Cleanup
        Throttler t = new AutoCompleteTestThrottler(this.containerId, wrap(calculator), executorService(), metrics, delays::add);

        // Set a non-maximum delay and ask to throttle, then verify we throttled the correct amount.
        calculator.setDelayMillis(NON_MAX_THROTTLE_MILLIS);
        t.throttle().get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Expected exactly one delay to be recorded.", 1, delays.size());
        Assert.assertEquals("Unexpected delay recorded.", NON_MAX_THROTTLE_MILLIS, (int) delays.get(0));
    }

    /**
     * Tests the case when {@link ThrottlerCalculator#getThrottlingDelay()} returns a value which requires repeated
     * delays (Maximum Delay == True).
     *
     * @throws Exception
     */
    @Test
    public void testMaximumDelay() throws Exception {
        final int repeatCount = 3;
        val delays = Collections.<Integer>synchronizedList(new ArrayList<>());
        val calculator = new TestCalculatorThrottler(THROTTLER_NAME);

        val nextDelay = new AtomicInteger(MAX_THROTTLE_MILLIS + repeatCount - 1);
        Consumer<Integer> recordDelay = delayMillis -> {
            delays.add(delayMillis);
            calculator.setDelayMillis(nextDelay.decrementAndGet());
        };

        // Request a throttling delay. Since we begin with a value higher than the MAX, we expect the throttler to
        // block as long as necessary; at each throttle cycle it should check the calculator for a new value, which we
        // will decrease an expect to unblock once we got a value smaller than MAX.
        @Cleanup
        Throttler t = new AutoCompleteTestThrottler(this.containerId, wrap(calculator), executorService(), metrics, recordDelay);
        calculator.setDelayMillis(nextDelay.get());
        t.throttle().get(SHORT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        Assert.assertEquals("Unexpected number of delays recorded.", repeatCount, delays.size());
        for (int i = 0; i < repeatCount; i++) {
            int expectedDelay = MAX_THROTTLE_MILLIS + Math.min(0, repeatCount - i); // Delays are capped.
            Assert.assertEquals("Unexpected delay recorded for step " + i, expectedDelay, (int) delays.get(i));
        }
    }

    /**
     * Tests the case when an interruption is called on an uninterruptible throttling source.
     *
     * @throws Exception
     */
    @Test
    public void testUninterruptibleDelay() throws Exception {
        // Supply monotonically decreasing delays.
        val suppliedDelays = Arrays.asList(2000);
        val delays = Collections.<Integer>synchronizedList(new ArrayList<>());
        val calculator = new TestCalculatorThrottler(ThrottlerCalculator.ThrottlerName.Batching);
        val nextDelay = suppliedDelays.iterator();
        Consumer<Integer> recordDelay = delayMillis -> {
            delays.add(delayMillis);
            calculator.setDelayMillis(nextDelay.hasNext() ? nextDelay.next() : 0); // 0 means we're done (no more throttling).
        };
        @Cleanup
        TestThrottler t = new TestThrottler(this.containerId, wrap(calculator), executorService(), metrics, recordDelay);

        // Set a non-maximum delay and ask to throttle, then verify we throttled the correct amount.
        calculator.setDelayMillis(nextDelay.next());
        val t1 = t.throttle();

        Assert.assertFalse("Not expected throttle future to be completed yet.", t1.isDone());
        t.notifyThrottleSourceChanged();

        TestUtils.await(t1::isDone, 5, TIMEOUT_MILLIS);

        Assert.assertEquals(
                "Last reported delay is equal to last supplied delay.",
                (int) suppliedDelays.get(0),
                (int) getThrottlerMetric(calculator.getName())
        );
    }

    /**
     * Tests interruptible Cache delays.
     *
     * @throws Exception
     */
    @Test
    public void testInterruptedCacheDelay() throws Exception {
        testInterruptedDelay(ThrottlerCalculator.ThrottlerName.Cache);
    }

    /**
     * Tests interruptible DurableDataLog delays.
     *
     * @throws Exception
     */
    @Test
    public void testInterruptedDurableDataLogDelay() throws Exception {
        testInterruptedDelay(ThrottlerCalculator.ThrottlerName.DurableDataLog);
    }

    /**
     * Tests if interruptible throttlers are correctly reporting the time spent throttled.
     *
     * @throws Exception
     */
    @Test
    public void testInterruptedIncreasingDelayMetrics() throws Exception {
        // Supply monotonically decreasing delays.
        val suppliedDelays = Arrays.asList(3000, 4000, 5000);
        val delays = Collections.<Integer>synchronizedList(new ArrayList<>());
        val calculator = new TestCalculatorThrottler(ThrottlerCalculator.ThrottlerName.Cache);
        val nextDelay = suppliedDelays.iterator();
        Consumer<Integer> recordDelay = delayMillis -> {
            delays.add(delayMillis);
            calculator.setDelayMillis(nextDelay.hasNext() ? nextDelay.next() : 0); // 0 means we're done (no more throttling).
        };
        @Cleanup
        TestThrottler t = new TestThrottler(this.containerId, wrap(calculator), executorService(), metrics, recordDelay);

        // Set a non-maximum delay and ask to throttle, then verify we throttled the correct amount.
        calculator.setDelayMillis(nextDelay.next());
        val t1 = t.throttle();
        Assert.assertFalse("Not expected throttle future to be completed yet.", t1.isDone());

        // For every delay that we want to submit, notify that the cache cleanup has completed, which should cancel the
        // currently running throttle cycle and request the next throttling value.
        for (int i = 1; i < suppliedDelays.size(); i++) {
            // Interrupt the current throttle cycle.
            t.notifyThrottleSourceChanged();
            Assert.assertFalse("Not expected throttle future to be completed yet.", t1.isDone());

            // Wait for the new cycle to begin (we use the recordDelay consumer above to figure this out).
            int expectedDelayCount = i + 1;
            TestUtils.await(() -> delays.size() == expectedDelayCount, 5, TIMEOUT_MILLIS);
        }
        TestUtils.await(t1::isDone, 5, TIMEOUT_MILLIS);

        // Because the supplied delays is monotonically decreasing, only the first delay value should be used to calculate
        // the duration supplied.
        AssertExtensions.assertLessThanOrEqual(
                "Throttler should be at most the first supplied delay",
                suppliedDelays.get(0),
                (int) getThrottlerMetric(calculator.getName())
        );
    }

    /**
     * Tests if interruptible throttlers are correctly reporting the time spent throttled.
     *
     * @throws Exception
     */
    @Test
    public void testInterruptedDecreasingDelayMetrics() throws Exception {
        // Supply monotonically decreasing delays.
        val suppliedDelays = Arrays.asList(5000, 4000, 3000);
        val delays = Collections.<Integer>synchronizedList(new ArrayList<>());
        val calculator = new TestCalculatorThrottler(ThrottlerCalculator.ThrottlerName.Cache);
        val nextDelay = suppliedDelays.iterator();
        Consumer<Integer> recordDelay = delayMillis -> {
            delays.add(delayMillis);
            calculator.setDelayMillis(nextDelay.hasNext() ? nextDelay.next() : 0); // 0 means we're done (no more throttling).
        };
        @Cleanup
        TestThrottler t = new TestThrottler(this.containerId, wrap(calculator), executorService(), metrics, recordDelay);

        // Set a non-maximum delay and ask to throttle, then verify we throttled the correct amount.
        calculator.setDelayMillis(nextDelay.next());
        val t1 = t.throttle();
        Assert.assertFalse("Not expected throttle future to be completed yet.", t1.isDone());
        // For every delay that we want to submit, notify that the cache cleanup has completed, which should cancel the
        // currently running throttle cycle and request the next throttling value.
        for (int i = 1; i < suppliedDelays.size(); i++) {
            // Interrupt the current throttle cycle.
            t.notifyThrottleSourceChanged();
            // Wait for the new cycle to begin (we use the recordDelay consumer above to figure this out).
            int expectedDelayCount = i + 1;
            TestUtils.await(() -> delays.size() == expectedDelayCount, 5, TIMEOUT_MILLIS);
        }
        TestUtils.await(t1::isDone, 5, TIMEOUT_MILLIS);
        // Because the supplied delays is monotonically decreasing, only the first delay value should be used to calculate
        // the duration supplied.
        AssertExtensions.assertGreaterThanOrEqual(
                "Excepted delay to be at least the smallest value.",
                 suppliedDelays.get(2),
                (int) getThrottlerMetric(calculator.getName())
        );
        AssertExtensions.assertLessThan(
                "Excepted delay to be strictly less than the max.",
                 suppliedDelays.get(0),
                (int) getThrottlerMetric(calculator.getName())
        );
    }

    /**
     * Tests the case when {@link Throttler#throttle()} returns a delay that can be interrupted using {@link Throttler#notifyThrottleSourceChanged()}}.
     */
    private void testInterruptedDelay(ThrottlerCalculator.ThrottlerName throttlerName) throws Exception {
        val suppliedDelays = Arrays.asList(5000, 2500, 5000);
        val delays = Collections.<Integer>synchronizedList(new ArrayList<>());
        val calculator = new TestCalculatorThrottler(throttlerName);
        val nextDelay = suppliedDelays.iterator();
        Consumer<Integer> recordDelay = delayMillis -> {
            delays.add(delayMillis);
            calculator.setDelayMillis(nextDelay.hasNext() ? nextDelay.next() : 0); // 0 means we're done (no more throttling).
        };
        @Cleanup
        TestThrottler t = new TestThrottler(this.containerId, wrap(calculator), executorService(), metrics, recordDelay);

        // Set a non-maximum delay and ask to throttle, then verify we throttled the correct amount.
        calculator.setDelayMillis(nextDelay.next());
        val t1 = t.throttle();
        Assert.assertFalse("Not expected throttle future to be completed yet.", t1.isDone());

        // For every delay that we want to submit, notify that the cache cleanup has completed, which should cancel the
        // currently running throttle cycle and request the next throttling value.
        for (int i = 1; i < suppliedDelays.size(); i++) {
            // Interrupt the current throttle cycle.
            t.notifyThrottleSourceChanged();
            Assert.assertFalse("Not expected throttle future to be completed yet.", t1.isDone());

            // Wait for the new cycle to begin (we use the recordDelay consumer above to figure this out).
            int expectedDelayCount = i + 1;
            TestUtils.await(() -> delays.size() == expectedDelayCount, 5, TIMEOUT_MILLIS);
        }

        // When we are done, complete the last throttle cycle and check final results.
        t.completeDelayFuture();
        TestUtils.await(t1::isDone, 5, TIMEOUT_MILLIS);
        Assert.assertEquals("Unexpected number of delays recorded.", suppliedDelays.size(), delays.size());

        Assert.assertEquals("Unexpected first delay value.", suppliedDelays.get(0), delays.get(0));

        // Subsequent delays cannot be predicted due to them being real time values and, as such, vary greatly between
        // runs and different environments. We can only check that they decrease in value (by design).
        for (int i = 1; i < delays.size(); i++) {
            AssertExtensions.assertLessThanOrEqual("Expected delays to be decreasing.", delays.get(i - 1), delays.get(i));
        }
    }

    /**
     * Test the throttleOnce method to check its behavior when throttler is suspended
     */
    @Test
    public void testThrottleOnceWhenSuspended() {
        val delays = new ArrayList<>();
        val calculator = new TestCalculatorThrottler(ThrottlerCalculator.ThrottlerName.Cache);
        calculator.setDelayMillis(10000);
        val suspended = new AtomicBoolean(true);
        TestThrottler t = new TestThrottler(this.containerId, wrap(calculator), suspended::get, executorService(), metrics, delays::add);
        t.throttleOnce(wrap(calculator).getThrottlingDelay());
        Assert.assertTrue("Error: Throttler has continued to throttle inspite of there being System-Critical to process", t.lastDelayFuture.get().isCompletedExceptionally());
    }

    /**
     * Test the throttleOnce method to check its behavior when throttler is not suspended
     */
    @Test
    public void testThrottleOnceWhenNotSuspended() throws Exception {
        val delays = new ArrayList<>();
        val calculator = new TestCalculatorThrottler(ThrottlerCalculator.ThrottlerName.Cache);
        int delayMillis = 500;
        calculator.setDelayMillis(delayMillis);
        val suspended = new AtomicBoolean(false);
        TestThrottler t = new TestThrottler(this.containerId, wrap(calculator), suspended::get, executorService(), metrics, delays::add);
        CompletableFuture<Void> throt = t.throttleOnce(wrap(calculator).getThrottlingDelay());
        TestUtils.await(throt::isDone, 5, TIMEOUT_MILLIS);
        Assert.assertFalse("Error: Throttler has not throttled accoriding to expected delay.", t.lastDelayFuture.get().isCompletedExceptionally());
    }


    /**
     * Tests the throttler when it is temporarily disabled.
     */
    @Test
    public void testTemporaryDisabled() throws Exception {
        testTemporaryDisabled(10000); // Non-max delay.
        testTemporaryDisabled(DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue()); // Max delay (different code path).
    }

    private void testTemporaryDisabled(int delayMillis) throws Exception {
        val delays = new ArrayList<>();
        val calculator = new TestCalculatorThrottler(ThrottlerCalculator.ThrottlerName.Cache);

        val disabled = new AtomicBoolean(true);
        @Cleanup
        TestThrottler t = new TestThrottler(this.containerId, wrap(calculator), disabled::get, executorService(), metrics, delays::add);

        // Test 1: Do not throttle if temporarily disabled.
        calculator.setDelayMillis(delayMillis);
        val disabledThrottle = t.throttle();
        Assert.assertTrue("Expected throttle() result to be completed for disabled throttle.", disabledThrottle.isDone());

        // Test 2: Interrupt the current throttle cycle.
        disabled.set(false);
        val t1 = t.throttle();
        Assert.assertFalse("Not expected non-disabled throttle to be completed yet.", t1.isDone());
        disabled.set(true);
        Assert.assertFalse("Not expected throttle future to be completed yet.", t1.isDone());
        t.notifyThrottleSourceChanged();
        TestUtils.await(t1::isDone, 5, TIMEOUT_MILLIS);

        // Test 2: When the current delay future completes normally.
        disabled.set(false);
        val t2 = t.throttle();
        t.awaitCreateDelayFuture(TIMEOUT_MILLIS);
        Assert.assertFalse("Not expected non-disabled throttle to be completed yet.", t2.isDone());
        disabled.set(true);
        Assert.assertFalse("Not expected throttle future to be completed yet.", t2.isDone());

        // We don't want to wait the actual timeout. Complete it now and check the result.
        t.completeDelayFuture();
        TestUtils.await(t2::isDone, 5, TIMEOUT_MILLIS);
    }

    private ThrottlerCalculator wrap(ThrottlerCalculator.Throttler calculatorThrottler) {
        return ThrottlerCalculator.builder().maxDelayMillis(DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue()).throttler(calculatorThrottler).build();
    }

    private double getThrottlerMetric(ThrottlerCalculator.ThrottlerName name) {
        return MetricRegistryUtils.getGauge(MetricsNames.OPERATION_PROCESSOR_DELAY_MILLIS, MetricsTags.throttlerTag(containerId, name.toString())).value();
    }

    @Test
    public void throttlerConfigTest() {
        // Check that default values are respected for ThrottlerConfig.
        DurableLogConfig durableLogConfig = DurableLogConfig.builder().build();
        ThrottlerPolicy throttlerPolicy = new ThrottlerPolicy(durableLogConfig);
        Assert.assertEquals((int) DurableLogConfig.MAX_BATCHING_DELAY_MILLIS.getDefaultValue(), throttlerPolicy.getMaxBatchingDelayMillis());
        Assert.assertEquals((int) DurableLogConfig.MAX_DELAY_MILLIS.getDefaultValue(), throttlerPolicy.getMaxDelayMillis());
        Assert.assertEquals((int) DurableLogConfig.OPERATION_LOG_MAX_SIZE.getDefaultValue(), throttlerPolicy.getOperationLogMaxSize());
        Assert.assertEquals((int) DurableLogConfig.OPERATION_LOG_TARGET_SIZE.getDefaultValue(), throttlerPolicy.getOperationLogTargetSize());

        // Set non-default values and verify that ThrottlerConfig stores these correctly.
        durableLogConfig = DurableLogConfig.builder()
                .with(DurableLogConfig.MAX_BATCHING_DELAY_MILLIS, 10)
                .with(DurableLogConfig.MAX_DELAY_MILLIS, 10000)
                .with(DurableLogConfig.OPERATION_LOG_MAX_SIZE, 1234)
                .with(DurableLogConfig.OPERATION_LOG_TARGET_SIZE, 123)
                .build();
        throttlerPolicy = new ThrottlerPolicy(durableLogConfig);
        Assert.assertEquals(10, throttlerPolicy.getMaxBatchingDelayMillis());
        Assert.assertEquals(10000, throttlerPolicy.getMaxDelayMillis());
        Assert.assertEquals(1234, throttlerPolicy.getOperationLogMaxSize());
        Assert.assertEquals(123, throttlerPolicy.getOperationLogTargetSize());

        // Check that we cannot set an OperationLog target size higher than the max size.
        AssertExtensions.assertThrows(ConfigurationException.class, () -> DurableLogConfig.builder()
                .with(DurableLogConfig.OPERATION_LOG_MAX_SIZE, 10)
                .with(DurableLogConfig.OPERATION_LOG_TARGET_SIZE, 20)
                .build());
    }

    //region Helper Classes

    @RequiredArgsConstructor
    @Getter
    @Setter
    private static class TestCalculatorThrottler extends ThrottlerCalculator.Throttler {
        private final ThrottlerCalculator.ThrottlerName name;
        private boolean throttlingRequired;
        private int delayMillis;
    }

    private static class TestThrottler extends Throttler {
        private final Consumer<Integer> newDelay;
        private final AtomicReference<CompletableFuture<Void>> lastDelayFuture = new AtomicReference<>();

        TestThrottler(int containerId, ThrottlerCalculator calculator, ScheduledExecutorService executor,
                      SegmentStoreMetrics.OperationProcessor metrics, Consumer<Integer> newDelay) {
            this(containerId, calculator, () -> false, executor, metrics, newDelay);
        }

        TestThrottler(int containerId, ThrottlerCalculator calculator, Supplier<Boolean> isDisabled, ScheduledExecutorService executor,
                      SegmentStoreMetrics.OperationProcessor metrics, Consumer<Integer> newDelay) {
            super(containerId, calculator, isDisabled, executor, metrics);
            this.newDelay = newDelay;
        }

        @Override
        protected CompletableFuture<Void> createDelayFuture(int millis) {
            this.newDelay.accept(millis);
            val oldDelay = this.lastDelayFuture.getAndSet(null);
            Assert.assertTrue(oldDelay == null || oldDelay.isDone());
            val result = super.createDelayFuture(millis);
            this.lastDelayFuture.set(result);
            return result;
        }

        void awaitCreateDelayFuture(int timeoutMillis) throws TimeoutException {
            TestUtils.await(() -> this.lastDelayFuture.get() != null, 5, timeoutMillis);
        }

        void completeDelayFuture() {
            val delayFuture = this.lastDelayFuture.getAndSet(null);
            Assert.assertNotNull(delayFuture);
            delayFuture.complete(null);
        }
    }

    /**
     * Overrides the {@link Throttler#createDelayFuture(int)} method to return a completed future and record the delay
     * requested. This is necessary to avoid having to wait indeterminate amounts of time to actually verify the throttling
     * mechanism.
     */
    private static class AutoCompleteTestThrottler extends TestThrottler {
        AutoCompleteTestThrottler(int containerId, ThrottlerCalculator calculator, ScheduledExecutorService executor,
                                  SegmentStoreMetrics.OperationProcessor metrics, Consumer<Integer> newDelay) {
            super(containerId, calculator, executor, metrics, newDelay);
        }

        @Override
        protected CompletableFuture<Void> createDelayFuture(int millis) {
            val delayFuture = super.createDelayFuture(millis);
            super.completeDelayFuture();
            return delayFuture;
        }
    }

    //endregion
}
