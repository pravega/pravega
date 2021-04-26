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

import io.pravega.common.Timer;
import io.pravega.test.common.SerializedClassRunner;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

/**
 * Test for Stats provider.
 */
@Slf4j
@RunWith(SerializedClassRunner.class)
public class MetricsProviderTest {

    private final StatsLogger statsLogger = MetricsProvider.createStatsLogger("testStatsLogger");
    private final DynamicLogger dynamicLogger = MetricsProvider.getDynamicLogger();

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

    /**
     * Test Event and Value registered and worked well with OpStats.
     */
    @Test
    public void testOpStatsData() {
        Timer startTime = new Timer();
        OpStatsLogger opStatsLogger = statsLogger.createStats("testOpStatsLogger");
        OpStatsLogger opStatsLogger1 = statsLogger.createStats("testOpStatsLogger", "containerId", "1");
        OpStatsLogger opStatsLogger2 = statsLogger.createStats("testOpStatsLogger", "containerId", "2");

        // register 2 event: 1 success, 1 fail.
        opStatsLogger.reportSuccessEvent(startTime.getElapsed());
        opStatsLogger.reportFailEvent(startTime.getElapsed());
        opStatsLogger.reportSuccessValue(startTime.getElapsedMillis());
        opStatsLogger.reportFailValue(startTime.getElapsedMillis());

        opStatsLogger.reportSuccessValue(1);
        opStatsLogger.reportFailValue(1);
        opStatsLogger.reportSuccessValue(1);

        // with tag "containerId=1": report 1 success 2 fail
        opStatsLogger1.reportSuccessEvent(startTime.getElapsed());
        opStatsLogger1.reportFailEvent(startTime.getElapsed());
        opStatsLogger1.reportFailValue(startTime.getElapsedMillis());

        // with tag "containerId=2": report 3 success 1 fail
        opStatsLogger2.reportSuccessEvent(startTime.getElapsed());
        opStatsLogger2.reportFailEvent(startTime.getElapsed());
        opStatsLogger2.reportSuccessValue(startTime.getElapsedMillis());
        opStatsLogger2.reportSuccessValue(startTime.getElapsedMillis());

        OpStatsData statsData = opStatsLogger.toOpStatsData();
        OpStatsData statsData1 = opStatsLogger1.toOpStatsData();
        OpStatsData statsData2 = opStatsLogger2.toOpStatsData();

        assertEquals(4, statsData.getNumSuccessfulEvents());
        assertEquals(3, statsData.getNumFailedEvents());

        assertEquals(1, statsData1.getNumSuccessfulEvents());
        assertEquals(2, statsData1.getNumFailedEvents());

        assertEquals(3, statsData2.getNumSuccessfulEvents());
        assertEquals(1, statsData2.getNumFailedEvents());
    }

    /**
     * Test counter registered and  worked well with StatsLogger.
     */
    @Test
    public void testCounter() {
        Counter testCounter = statsLogger.createCounter("testCounter");
        Counter testCounter1 = statsLogger.createCounter("testCounter", "tag1", "value1");
        Counter testCounter2 = statsLogger.createCounter("testCounter", "tag2", "value2");

        testCounter.add(17);
        testCounter1.add(11);
        testCounter2.add(22);

        assertEquals(17, testCounter.get());
        assertEquals(11, testCounter1.get());
        assertEquals(22, testCounter2.get());

        // test dynamic counter without tags
        int sum = 0;
        for (int i = 1; i < 10; i++) {
            sum += i;
            dynamicLogger.incCounterValue("dynamicCounterNoTag", i);
            assertEquals(sum, (int) MetricRegistryUtils.getCounter("dynamicCounterNoTag").count());
        }

        dynamicLogger.updateCounterValue("dynamicCounterNoTag", 100);
        assertEquals(100, (int) MetricRegistryUtils.getCounter("dynamicCounterNoTag").count());
        dynamicLogger.freezeCounter("dynamicCounterNoTag");
        assertEquals(null, MetricRegistryUtils.getCounter("dynamicCounterNoTag"));

        // test dynamic counter with tags
        sum = 0;
        for (int i = 1; i < 10; i++) {
            sum += i;
            dynamicLogger.incCounterValue("dynamicCounter", 2 * i, "hostname", "1.1.1.1");
            dynamicLogger.incCounterValue("dynamicCounter", 3 * i, "hostname", "2.2.2.2");
            assertEquals(2 * sum, (int) MetricRegistryUtils.getCounter("dynamicCounter", "hostname", "1.1.1.1").count());
            assertEquals(3 * sum, (int) MetricRegistryUtils.getCounter("dynamicCounter", "hostname", "2.2.2.2").count());
        }
        dynamicLogger.updateCounterValue("dynamicCounter", 200, "hostname", "1.1.1.1");
        assertEquals(200, (int) MetricRegistryUtils.getCounter("dynamicCounter", "hostname", "1.1.1.1").count());

        dynamicLogger.freezeCounter("dynamicCounter", "hostname", "1.1.1.1");
        dynamicLogger.freezeCounter("dynamicCounter", "hostname", "2.2.2.2");

        assertEquals(null, MetricRegistryUtils.getCounter("dynamicCounter", "hostname", "1.1.1.1"));
        assertEquals(null, MetricRegistryUtils.getCounter("dynamicCounter", "hostname", "2.2.2.2"));
    }

    /**
     * Test Meter registered and  worked well with StatsLogger.
     */
    @Test
    public void testMeter() {
        Meter testMeter = statsLogger.createMeter("testMeter");
        Meter testMeter1 = statsLogger.createMeter("testMeter", "containerId", "1");
        Meter testMeter2 = statsLogger.createMeter("testMeter", "containerId", "2");

        testMeter.recordEvent();
        testMeter.recordEvent();
        testMeter1.recordEvent();
        testMeter2.recordEvent();

        assertEquals(2, testMeter.getCount());
        assertEquals(1, testMeter1.getCount());
        assertEquals(1, testMeter2.getCount());

        testMeter.recordEvents(27);
        testMeter1.recordEvents(7);
        testMeter2.recordEvents(13);
        assertEquals(29, testMeter.getCount());
        assertEquals(8, testMeter1.getCount());
        assertEquals(14, testMeter2.getCount());

        // test dynamic meter without tags
        int sum = 0;
        for (int i = 1; i < 10; i++) {
            sum += i;
            dynamicLogger.recordMeterEvents("dynamicMeterNoTag", i);
            assertEquals(sum, (long) MetricRegistryUtils.getMeter("dynamicMeterNoTag").totalAmount());
        }

        // test dynamic meter with tags
        sum = 0;
        for (int i = 1; i < 10; i++) {
            sum += i;
            dynamicLogger.recordMeterEvents("dynamicMeter", 2 * i, "container", "1");
            dynamicLogger.recordMeterEvents("dynamicMeter", 3 * i, "container", "2");
            assertEquals(2 * sum, (long) MetricRegistryUtils.getMeter("dynamicMeter", "container", "1").totalAmount());
            assertEquals(3 * sum, (long) MetricRegistryUtils.getMeter("dynamicMeter", "container", "2").totalAmount());
        }

        dynamicLogger.freezeMeter("dynamicMeter", "container", "1");
        dynamicLogger.freezeMeter("dynamicMeter", "container", "2");
        assertEquals(null, MetricRegistryUtils.getMeter("dynamicMeter", "container", "1"));
        assertEquals(null, MetricRegistryUtils.getMeter("dynamicMeter", "container", "2"));
    }

    /**
     * Test gauge registered and  worked well with StatsLogger.
     */
    @Test
    public void testGauge() {

        // test gauge value supplier
        String[] tags1 = {"tagKey", "tagValue"};
        Gauge gauge1 = statsLogger.registerGauge("testGaugeFunctionNoTag", () -> 23);
        Gauge gauge2 = statsLogger.registerGauge("testGaugeFunction", () -> 52, tags1);
        assertEquals(23, (int) MetricRegistryUtils.getGauge("testGaugeFunctionNoTag").value());
        assertEquals(52, (int) MetricRegistryUtils.getGauge("testGaugeFunction", tags1).value());
        gauge1.setSupplier(() -> 32);
        gauge2.setSupplier(() -> 25);
        assertEquals(32, (int) MetricRegistryUtils.getGauge("testGaugeFunctionNoTag").value());
        assertEquals(25, (int) MetricRegistryUtils.getGauge("testGaugeFunction", tags1).value());

        AtomicInteger value = new AtomicInteger(1);
        AtomicInteger value1 = new AtomicInteger(100);
        AtomicInteger value2 = new AtomicInteger(200);
        statsLogger.registerGauge("testGaugeNoTag", value::get);
        statsLogger.registerGauge("testGaugeTags", value1::get, "container", "1");
        statsLogger.registerGauge("testGaugeTags", value2::get, "container", "2");

        value.set(2);
        value1.set(3);
        value2.set(4);
        assertEquals(value.get(), (int) MetricRegistryUtils.getGauge("testGaugeNoTag").value());
        assertEquals(value1.get(), (int) MetricRegistryUtils.getGauge("testGaugeTags", "container", "1").value());
        assertEquals(value2.get(), (int) MetricRegistryUtils.getGauge("testGaugeTags", "container", "2").value());

        // test dynamic Gauge without tags
        for (int i = 1; i < 10; i++) {
            dynamicLogger.reportGaugeValue("dynamicGaugeNoTag", i);
            assertEquals(i, (int) MetricRegistryUtils.getGauge("dynamicGaugeNoTag").value());
        }

        dynamicLogger.freezeGaugeValue("dynamicGaugeNoTag");
        assertEquals(null, MetricRegistryUtils.getGauge("dynamicGaugeNoTag"));

        // test Gauge with tags
        for (int i = 1; i < 10; i++) {
            dynamicLogger.reportGaugeValue("dynamicGauge", 2 * i, "container", "1");
            dynamicLogger.reportGaugeValue("dynamicGauge", 3 * i, "container", "2");
            assertEquals(2 * i, (int) MetricRegistryUtils.getGauge("dynamicGauge", "container", "1").value());
            assertEquals(3 * i, (int) MetricRegistryUtils.getGauge("dynamicGauge", "container", "2").value());
        }

        dynamicLogger.freezeGaugeValue("dynamicGauge", "container", "1");
        dynamicLogger.freezeGaugeValue("dynamicGauge", "container", "2");
        assertEquals(null, MetricRegistryUtils.getGauge("dynamicGauge", "container", "1"));
        assertEquals(null, MetricRegistryUtils.getGauge("dynamicGauge", "container", "2"));
    }

    /**
     * Test that we can transition from stats enabled, to disabled, to enabled.
     */
    @Test
    public void testMultipleInitialization() {
        MetricsConfig config = MetricsConfig.builder()
                                            .with(MetricsConfig.ENABLE_STATISTICS, false)
                                            .build();
        MetricsProvider.initialize(config);
        statsLogger.createCounter("counterDisabled");

        assertEquals(null, MetricRegistryUtils.getCounter("counterDisabled"));

        config = MetricsConfig.builder()
                              .with(MetricsConfig.ENABLE_STATISTICS, true)
                              .build();
        MetricsProvider.initialize(config);
        statsLogger.createCounter("counterEnabled");

        Assert.assertNotNull(
                MetricRegistryUtils.getCounter("counterEnabled"));
    }

    /**
     * Test that we can transition from stats enabled, to disabled, to enabled.
     */
    @Test
    public void testContinuity() {
        statsLogger.createCounter("continuity-counter");
        Assert.assertNotNull("Not registered before disabling.",
                MetricRegistryUtils.getCounter("continuity-counter"));

        MetricsConfig disableConfig = MetricsConfig.builder()
                                            .with(MetricsConfig.ENABLE_STATISTICS, false)
                                            .build();
        MetricsProvider.initialize(disableConfig);
        Assert.assertNull("Still registered after disabling.",
                MetricRegistryUtils.getCounter("continuity-counter"));

        MetricsConfig enableConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .build();
        MetricsProvider.initialize(enableConfig);
        Assert.assertNotNull("Not registered after re-enabling.",
                MetricRegistryUtils.getCounter("continuity-counter"));
    }

    /**
     * Test transition back to null provider.
     */
    @Test
    public void testTransitionBackToNullProvider() {
        MetricsConfig config = MetricsConfig.builder()
                                            .with(MetricsConfig.ENABLE_STATISTICS, false)
                                            .build();
        MetricsProvider.initialize(config);

        Counter counter = statsLogger.createCounter("continuity-counter");
        counter.add(1L);
        assertEquals(0L, counter.get());

        config = MetricsConfig.builder()
                              .with(MetricsConfig.ENABLE_STATISTICS, true)
                              .build();
        MetricsProvider.initialize(config);

        counter.add(1L);
        assertEquals(1L, counter.get());
    }
}
