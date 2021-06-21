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

import io.pravega.shared.MetricsTags;
import io.pravega.test.common.SerializedClassRunner;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for basic metric type such as OpStatsLogger, Counter, Gauge and Meter
 */
@Slf4j
@RunWith(SerializedClassRunner.class)
public class BasicMetricTest {

    private final StatsLogger statsLogger = MetricsProvider.createStatsLogger("testStatsLogger");

    @Before
    public void setUp() {
        MetricsProvider.initialize(MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .build());
        MetricsProvider.getMetricsProvider().startWithoutExporting();
    }

    @After
    public void tearDown() {
        MetricsProvider.getMetricsProvider().close();
    }

    @Test
    public void testMetricClose() {
        OpStatsLogger opStatsLogger = statsLogger.createStats("testOpStatsLogger");
        Counter counter = statsLogger.createCounter("testCounter", "containerId", "1");
        Gauge gauge = statsLogger.registerGauge("testGauge", () -> 13, "containerId", "2");
        Meter meter = statsLogger.createMeter("testMeter", "containerId", "3");

        assertNotNull(MetricRegistryUtils.getTimer("testOpStatsLogger").getId());
        assertNotNull(MetricRegistryUtils.getCounter("testCounter", "containerId", "1").getId());
        assertNotNull(MetricRegistryUtils.getGauge("testGauge", "containerId", "2").getId());
        assertNotNull(MetricRegistryUtils.getMeter("testMeter", "containerId", "3").getId());

        assertNull(MetricRegistryUtils.getCounter("testOpStatsLogger"));
        assertNull(MetricRegistryUtils.getMeter("testOpStatsLogger"));
        assertNull(MetricRegistryUtils.getGauge("testOpStatsLogger"));

        opStatsLogger.close();
        counter.close();
        gauge.close();
        meter.close();

        assertNull(MetricRegistryUtils.getTimer("testOpStatsLogger"));
        assertNull(MetricRegistryUtils.getCounter("testCounter", "containerId", "1"));
        assertNull(MetricRegistryUtils.getGauge("testGauge", "containerId", "2"));
        assertNull(MetricRegistryUtils.getMeter("testMeter", "containerId", "3"));
    }

    @Test (expected = Exception.class)
    public void testOpStatsLoggerCreationException() {
        statsLogger.createStats("testOpStatsLogger", "tagNameOnly");
    }

    @Test (expected = Exception.class)
    public void testCounterCreationException() {
        statsLogger.createCounter("testCounter", "tagName1", "tagValue1", "tagName2");
    }

    @Test (expected = Exception.class)
    public void testMeterCreationException() {
        statsLogger.createMeter("testMeter", "tagName1", "tagValue1", "tagName2");
    }

    @Test (expected = Exception.class)
    public void testGaugeCreationTagException() {
        statsLogger.registerGauge("testGauge", () -> 5, "tagKey1", "tagValue1", "tagKey2");
    }

    @Test
    public void testGaugeCreationFunctionException() {
        Gauge noopGauge = statsLogger.registerGauge("testGauge", null, "tagKey", "tagValue");
        assertTrue(noopGauge.getId() == null);
        noopGauge.close();

    }

    @Test
    public void testCounterClear() {
        Counter testCounter = statsLogger.createCounter("testCounter", "key", "value");
        testCounter.add(13);
        assertTrue(13 == MetricRegistryUtils.getCounter("testCounter", "key", "value").count());
        testCounter.clear();
        testCounter.inc();
        assertTrue(1 == MetricRegistryUtils.getCounter("testCounter", "key", "value").count());
    }

    @Test
    public void testCommonHostTag() {
        Counter counter = statsLogger.createCounter("counterWithCommonTag", "key", "value");
        counter.add(117);
        assertNotNull(counter.getId().getTag(MetricsTags.TAG_HOST));
        assertTrue(117 == MetricRegistryUtils.getCounter("counterWithCommonTag", "key", "value", MetricsTags.TAG_HOST, counter.getId().getTag(MetricsTags.TAG_HOST)).count());
        assertTrue(117 == MetricRegistryUtils.getCounter("counterWithCommonTag", "key", "value").count());
        assertNull(MetricRegistryUtils.getCounter("counterWithCommonTag", "key", "value", MetricsTags.TAG_HOST, "non-exist"));
    }

    @Test
    public void testProxyRepeatedClose() {
        String counterName = "counterProxy";
        Counter counter = statsLogger.createCounter(counterName, "key", "value");

        AtomicBoolean flag = new AtomicBoolean();
        CounterProxy proxy = new CounterProxy(counter, counterName, val -> flag.set(!flag.get()));
        proxy.close();
        // Make sure its been deleted.
        assertNull(MetricRegistryUtils.getCounter(counterName, "key", "value"));
        // Attempt to close it again, and that the callback is not ran, i.e. the flag is not flipped and remains true.
        proxy.close();
        assertTrue("The flag has been flipped twice indicating the close() call was called twice.", flag.get());
        assertNotNull("The MetricProxy shoudl continue to hold a reference to the Metric when closed.", proxy.getInstance());
    }
}
