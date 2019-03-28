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

import io.micrometer.influx.InfluxConfig;
import io.micrometer.statsd.StatsdConfig;
import io.micrometer.statsd.StatsdFlavor;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests RegistryConfigUtil
 */
@Slf4j
public class RegistryConfigUtilTest {

    @Test
    public void testStatsdConfig() {
        MetricsConfig appConfig = MetricsConfig.builder()
                .with(MetricsConfig.OUTPUT_FREQUENCY, 37)
                .with(MetricsConfig.METRICS_PREFIX, "statsdPrefix")
                .with(MetricsConfig.STATSD_HOST, "localhost")
                .with(MetricsConfig.STATSD_PORT, 8225)
                .build();

        StatsdConfig testConfig = RegistryConfigUtil.createStatsdConfig(appConfig);
        assertTrue(37 == testConfig.step().getSeconds());
        assertEquals("statsdPrefix", testConfig.prefix());
        assertEquals("localhost", testConfig.host());
        assertTrue(8225 == testConfig.port());
        assertEquals(StatsdFlavor.TELEGRAF, testConfig.flavor());
        assertNull(testConfig.get("Undefined Key"));
    }

    @Test
    public void testInfluxConfig() {
        MetricsConfig appConfig = MetricsConfig.builder()
                .with(MetricsConfig.OUTPUT_FREQUENCY, 39)
                .with(MetricsConfig.METRICS_PREFIX, "influxPrefix")
                .with(MetricsConfig.INFLUXDB_URI, "http://localhost:2375")
                .with(MetricsConfig.INFLUXDB_NAME, "databaseName")
                .with(MetricsConfig.INFLUXDB_USERNAME, "admin")
                .with(MetricsConfig.INFLUXDB_PASSWORD, "changeme")
                .build();

        InfluxConfig testConfig = RegistryConfigUtil.createInfluxConfig(appConfig);
        assertTrue(39 == testConfig.step().getSeconds());
        assertEquals("influxPrefix", testConfig.prefix());
        assertEquals("http://localhost:2375", testConfig.uri());
        assertEquals("databaseName", testConfig.db());
        assertEquals("admin", testConfig.userName());
        assertEquals("changeme", testConfig.password());
        assertNull(testConfig.get("Undefined Key"));
    }
}
