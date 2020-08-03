/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.metrics;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class MeterRegisterTest {

    private void initMetrics() {
        MetricsConfig metricsConfig = MetricsConfig.builder().with(MetricsConfig.ENABLE_STATSD_REPORTER, false).build();
        metricsConfig.setDynamicCacheEvictionDuration(Duration.ofMinutes(5));
        MetricsProvider.initialize(metricsConfig);
        StatsProvider statsProvider = MetricsProvider.getMetricsProvider();
        statsProvider.startWithoutExporting();
    }
    
    @Test
    public void testMeterRegister() {
        initMetrics();
        CompositeMeterRegistry registry = Metrics.globalRegistry;
        Timer success = Timer.builder("name").tags().publishPercentiles(OpStatsData.PERCENTILE_ARRAY).register(registry);
        success.record(100, TimeUnit.MILLISECONDS);
        success.record(100, TimeUnit.MILLISECONDS);
        Assert.assertEquals(2, success.count());
        Timer latencyValues = MetricRegistryUtils.getTimer("name");
        Assert.assertNotNull(latencyValues);
        Assert.assertEquals(2, latencyValues.count());
    }
    
    @Test
    public void testMeterReRegister() {
        initMetrics();
        CompositeMeterRegistry registry = Metrics.globalRegistry;
        Timer success = Timer.builder("name").tags().publishPercentiles(OpStatsData.PERCENTILE_ARRAY).register(registry);
        success.record(100, TimeUnit.MILLISECONDS);
        success.record(100, TimeUnit.MILLISECONDS);
        success.close();
        
        Timer latencyValues = MetricRegistryUtils.getTimer("name");
        Assert.assertNotNull(latencyValues);
        Assert.assertEquals(2, latencyValues.count());
        
        initMetrics();
        success = Timer.builder("name").tags().publishPercentiles(OpStatsData.PERCENTILE_ARRAY).register(registry);
        success.record(100, TimeUnit.MILLISECONDS);
        success.record(100, TimeUnit.MILLISECONDS);
        success.record(100, TimeUnit.MILLISECONDS);

        latencyValues = MetricRegistryUtils.getTimer("name");
        Assert.assertNotNull(latencyValues);
        Assert.assertEquals(3, latencyValues.count());
    }
    
}
