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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.influx.InfluxMeterRegistry;
import io.micrometer.statsd.StatsdMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Tests for StatsProvider
 */
@Slf4j
public class StatsProviderTest {

    private final StatsLogger statsLogger = MetricsProvider.createStatsLogger("testStatsLogger");

    @Test
    public void testStatsProviderStartAndClose() {
        MetricsConfig appConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATSD_REPORTER, true)
                .with(MetricsConfig.ENABLE_INFLUXDB_REPORTER, false)
                .build();

        StatsProvider statsProvider = new StatsProviderImpl(appConfig);
        statsProvider.start();

        for (MeterRegistry registry : Metrics.globalRegistry.getRegistries()) {
            assertFalse(registry instanceof InfluxMeterRegistry);
            assertTrue(registry instanceof StatsdMeterRegistry);
        }

        statsProvider.close();
        assertTrue(0 == Metrics.globalRegistry.getRegistries().size());
    }

    @Test
    public void testStatsProviderStartWithoutExporting() {
        MetricsConfig appConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATSD_REPORTER, true)
                .with(MetricsConfig.ENABLE_INFLUXDB_REPORTER, true)
                .build();

        StatsProvider statsProvider = new StatsProviderImpl(appConfig);
        statsProvider.startWithoutExporting();

        for (MeterRegistry registry : Metrics.globalRegistry.getRegistries()) {
            assertTrue(registry instanceof SimpleMeterRegistry);
            assertFalse(registry instanceof InfluxMeterRegistry);
            assertFalse(registry instanceof StatsdMeterRegistry);
        }

        statsProvider.close();
        assertTrue(0 == Metrics.globalRegistry.getRegistries().size());
    }

    @Test (expected = Exception.class)
    public void testStatsProviderNoRegisterBound() {
        MetricsConfig appConfig = MetricsConfig.builder()
                .with(MetricsConfig.ENABLE_STATSD_REPORTER, false)
                .with(MetricsConfig.ENABLE_INFLUXDB_REPORTER, false)
                .build();

        StatsProvider statsProvider = new StatsProviderImpl(appConfig);
        statsProvider.start();
    }
}
