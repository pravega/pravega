/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

import io.pravega.shared.MetricsTags;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

/**
 * Tests for basic metric type such as OpStatsLogger, Counter, Gauge and Meter
 */
@Slf4j
public class BasicMetricTest {

    private final StatsLogger statsLogger = MetricsProvider.createStatsLogger("testStatsLogger");

    @Before
    public void setUp() {
        MetricsProvider.initialize(MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATISTICS, true)
                .build());
        MetricsProvider.getMetricsProvider().startWithoutExporting();
    }

    @Test
    public void testMetricClose() {
        OpStatsLogger opStatsLogger = statsLogger.createStats("testOpStatsLogger");
        Counter counter = statsLogger.createCounter("testCounter", "containerId", "1");
        Gauge gauge = statsLogger.registerGauge("testGauge", () -> 13, "containerId", "2");
        Meter meter = statsLogger.createMeter("testMeter", "containerId", "3");

        assertNotNull(MetricRegistryUtils.getTimer("pravega.testStatsLogger.testOpStatsLogger").getId());
        assertNotNull(MetricRegistryUtils.getCounter("pravega.testStatsLogger.testCounter", "containerId", "1").getId());
        assertNotNull(MetricRegistryUtils.getGauge("pravega.testStatsLogger.testGauge", "containerId", "2").getId());
        assertNotNull(MetricRegistryUtils.getMeter("pravega.testStatsLogger.testMeter", "containerId", "3").getId());

        assertNull(MetricRegistryUtils.getCounter("pravega.testStatsLogger.testOpStatsLogger"));
        assertNull(MetricRegistryUtils.getMeter("pravega.testStatsLogger.testOpStatsLogger"));
        assertNull(MetricRegistryUtils.getGauge("pravega.testStatsLogger.testOpStatsLogger"));

        opStatsLogger.close();
        counter.close();
        gauge.close();
        meter.close();

        assertNull(MetricRegistryUtils.getTimer("pravega.testStatsLogger.testOpStatsLogger"));
        assertNull(MetricRegistryUtils.getCounter("pravega.testStatsLogger.testCounter", "containerId", "1"));
        assertNull(MetricRegistryUtils.getGauge("pravega.testStatsLogger.testGauge", "containerId", "2"));
        assertNull(MetricRegistryUtils.getMeter("pravega.testStatsLogger.testMeter", "containerId", "3"));
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
    public void testGaugeCreationException() {
        statsLogger.registerGauge("testGauge", () -> 5, "tagName1", "tagValue1", "tagName2");
    }

    @Test
    public void testCounterClear() {
        Counter testCounter = statsLogger.createCounter("testCounter", "key", "value");
        testCounter.add(13);
        assertTrue(13 == MetricRegistryUtils.getCounter("pravega.testStatsLogger.testCounter", "key", "value").count());
        testCounter.clear();
        testCounter.inc();
        assertTrue(1 == MetricRegistryUtils.getCounter("pravega.testStatsLogger.testCounter", "key", "value").count());
    }

    @Test
    public void testCommonHostTag() {
        Counter counter = statsLogger.createCounter("counterWithCommonTag", "key", "value");
        counter.add(117);
        assertNotNull(counter.getId().getTag(MetricsTags.TAG_HOST));
        assertTrue(117 == MetricRegistryUtils.getCounter("pravega.testStatsLogger.counterWithCommonTag", "key", "value", MetricsTags.TAG_HOST, counter.getId().getTag(MetricsTags.TAG_HOST)).count());
        assertTrue(117 == MetricRegistryUtils.getCounter("pravega.testStatsLogger.counterWithCommonTag", "key", "value").count());
        assertNull(MetricRegistryUtils.getCounter("pravega.testStatsLogger.counterWithCommonTag", "key", "value", MetricsTags.TAG_HOST, "non-exist"));
    }
}
