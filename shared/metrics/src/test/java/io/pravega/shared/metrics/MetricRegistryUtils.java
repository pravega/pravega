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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.DistributionSummary;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricRegistryUtils {

    public static Counter getCounter(String metricsName, String... tags) {
        return Metrics.globalRegistry.find(metricsName).tags(tags).counter();
    }

    public static DistributionSummary getMeter(String metricsName, String... tags) {
        return Metrics.globalRegistry.find(metricsName).tags(tags).summary();
    }

    public static Gauge getGauge(String metricsName, String... tags) {
        return Metrics.globalRegistry.find(metricsName).tags(tags).gauge();
    }

    public static Timer getTimer(String metricsName, String... tags) {
        return Metrics.globalRegistry.find(metricsName).tags(tags).timer();
    }
}
