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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.Timer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricRegistryUtils {

    public static Counter getCounter(String metricsName) {
        return (Counter) getMetric(metricsName);
    }

    public static Meter getMeter(String metricsName) {
        return (Meter) getMetric(metricsName);
    }

    public static Gauge getGauge(String metricsName) {
        return (Gauge) getMetric(metricsName);
    }

    public static Timer getTimer(String metricsName) {
        return (Timer) getMetric(metricsName);
    }

    public static Metric getMetric(String metricsName) {
        Metric metric = MetricsProvider.getMetric(metricsName);
        if (metric == null) {
            log.info("The metric {} is not present in the Metrics Registry", metricsName);
            return null;
        }
        return metric;
    }
}
