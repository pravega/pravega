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
package io.pravega.shared.metrics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the StatsLoggerProxy class.
 */
public class StatsLoggerProxyTest {
    private static final int METRIC_COUNT = 4;

    /**
     * Verifies the behavior of setLogger with respect to registering/unregistering metrics.
     */
    @Test
    public void testSetLogger() {
        val metrics1 = new ArrayList<TestMetric>();
        val l1 = new TestLogger(metrics1::add);
        val proxy = new StatsLoggerProxy(l1);
        createMetrics(proxy, "");
        Assert.assertEquals("Unexpected number of metrics registered with first logger.", METRIC_COUNT, metrics1.size());
        Assert.assertFalse("Metrics were not supposed to be closed yet.", metrics1.stream().anyMatch(TestMetric::isClosed));

        //create metrics with tags
        createMetrics(proxy, "", "containerId", "12");
        Assert.assertEquals("Unexpected number of metrics registered with first logger.", 2 * METRIC_COUNT, metrics1.size());
        Assert.assertFalse("Metrics were not supposed to be closed yet.", metrics1.stream().anyMatch(TestMetric::isClosed));

        // Update the logger and verify old instances are closed.
        val metrics2 = new ArrayList<TestMetric>();
        val l2 = new TestLogger(metrics2::add);
        proxy.setLogger(l2);
        Assert.assertFalse("Metrics were not supposed to be closed yet.", metrics2.stream().anyMatch(TestMetric::isClosed));
        Assert.assertEquals("Unexpected number of metrics registered with second logger.", metrics1.size(), metrics2.size());
        Assert.assertTrue("Old metrics were not closed when logger changed.", metrics1.stream().allMatch(TestMetric::isClosed));
    }

    /**
     * Tests the ability to re-use metrics if they're already cached.
     */
    @Test
    public void testGetOrCreate() {
        val metrics = new ArrayList<TestMetric>();
        val logger = new TestLogger(metrics::add);
        val proxy = new StatsLoggerProxy(logger);
        createMetrics(proxy, "");
        Assert.assertEquals("Unexpected number of metrics registered initially.", METRIC_COUNT, metrics.size());

        // Create metrics with same name but additional tag
        createMetrics(proxy, "", "hostname", "localhost");
        Assert.assertEquals("Unexpected number of metrics registered initially.", METRIC_COUNT * 2, metrics.size());

        // Re-create/request the same metrics. Verify the original ones have not been touched.
        createMetrics(proxy, "");
        Assert.assertEquals("Unexpected number of metrics registered after re-registering same names.", 2 * METRIC_COUNT, metrics.size());
        Assert.assertFalse("Original metrics were not supposed to be closed.", metrics.subList(0, 2 * METRIC_COUNT).stream().anyMatch(TestMetric::isClosed));

        // Create a new set of metrics and verify the original ones were not touched.
        createMetrics(proxy, "foo");
        Assert.assertEquals("Unexpected number of metrics registered after adding new ones.", 3 * METRIC_COUNT, metrics.size());
        Assert.assertFalse("Original metrics were not supposed to be closed.", metrics.subList(0, 2 * METRIC_COUNT).stream().anyMatch(TestMetric::isClosed));
        Assert.assertFalse("Newly-added metrics were not supposed to be closed.",
                metrics.stream().skip(3 * METRIC_COUNT).anyMatch(TestMetric::isClosed));

        createMetrics(proxy, "foo", "containerId", "6");
        Assert.assertEquals("Unexpected number of metrics registered after adding new ones.", 4 * METRIC_COUNT, metrics.size());
        Assert.assertFalse("Original metrics were not supposed to be closed.", metrics.subList(0, 2 * METRIC_COUNT).stream().anyMatch(TestMetric::isClosed));
        Assert.assertFalse("Newly-added metrics were not supposed to be closed.",
                metrics.stream().skip(3 * METRIC_COUNT).anyMatch(TestMetric::isClosed));
    }

    @Test
    public void testCloseCreate() {
        // Verify counter is being closed and then re-created when requested.
        val metrics = new ArrayList<TestMetric>();
        val logger = new TestLogger(metrics::add);
        val proxy = new StatsLoggerProxy(logger);
        val proxies = createMetrics(proxy, "");
        proxies.forEach(Metric::close);
        Assert.assertTrue("Expected all metrics to be closed.", metrics.stream().allMatch(TestMetric::isClosed));

        // Re-create them and verify new metric instances are created.
        createMetrics(proxy, "");
        Assert.assertEquals("Unexpected number of metrics registered after adding new ones.", 2 * METRIC_COUNT, metrics.size());
        Assert.assertFalse("Newly-added metrics were not supposed to be closed.",
                metrics.stream().skip(METRIC_COUNT).anyMatch(TestMetric::isClosed));
    }

    private Collection<Metric> createMetrics(StatsLoggerProxy proxy, String suffix, String... tags) {
        return Arrays.asList(
                proxy.createStats("stats" + suffix, tags),
                proxy.createCounter("counter" + suffix, tags),
                proxy.createMeter("meter" + suffix, tags),
                proxy.registerGauge("gauge" + suffix, () -> 1, tags)
                );
    }

    private static class TestLogger implements StatsLogger {
        private final Consumer<TestMetric> onCreate;

        TestLogger(Consumer<TestMetric> onCreate) {
            this.onCreate = onCreate;
        }

        @Override
        public OpStatsLogger createStats(String name, String... tags) {
            return create(name, tags);
        }

        @Override
        public Counter createCounter(String name, String... tags) {
            return create(name, tags);
        }

        @Override
        public Meter createMeter(String name, String... tags) {
            return create(name, tags);
        }

        @Override
        public Gauge registerGauge(String name, Supplier<Number> supplier, String... tags) {
            return create(name, tags);
        }

        @Override
        public StatsLogger createScopeLogger(String scope) {
            return this;
        }

        private TestMetric create(String name, String... tags) {
            val r = new TestMetric(name, tags);
            this.onCreate.accept(r);
            return r;
        }
    }

    private static class TestMetric implements OpStatsLogger, Counter, Meter, Gauge {
        @Getter
        private final io.micrometer.core.instrument.Meter.Id id;
        @Getter
        private boolean closed;

        TestMetric(String metricName, String... tagPairs) {
            this.id = new io.micrometer.core.instrument.Meter.Id("prefix-" + metricName,
                    io.micrometer.core.instrument.Tags.of(tagPairs),
                    null, null,
                    io.micrometer.core.instrument.Meter.Type.OTHER);
        }

        @Override
        public void close() {
            this.closed = true;
        }

        //region Not Implemented

        @Override
        public void inc() {

        }

        @Override
        public void add(long delta) {

        }

        @Override
        public long get() {
            return 0;
        }

        @Override
        public void recordEvent() {

        }

        @Override
        public void recordEvents(long n) {

        }

        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public void reportSuccessEvent(Duration duration) {

        }

        @Override
        public void reportFailEvent(Duration duration) {

        }

        @Override
        public void reportSuccessValue(long value) {

        }

        @Override
        public void reportFailValue(long value) {

        }

        @Override
        public OpStatsData toOpStatsData() {
            return null;
        }

        @Override
        public void clear() {

        }

        @Override
        public Supplier<Number> getSupplier() {
            return () -> 5;
        }

        @Override
        public void setSupplier(Supplier<Number> supplier) {

        }
        //endregion
    }
}
