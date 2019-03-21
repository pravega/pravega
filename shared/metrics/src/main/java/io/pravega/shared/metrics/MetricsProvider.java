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

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;

public class MetricsProvider {

    static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
    private static final StatsProviderProxy STATS_PROVIDER = new StatsProviderProxy();
    private static final DynamicLoggerProxy DYNAMIC_LOGGER = new DynamicLoggerProxy(STATS_PROVIDER.createDynamicLogger());

    @VisibleForTesting
    public static Metric getMetric(String name) {
        return  METRIC_REGISTRY.getMetrics().getOrDefault(name, null);
    }

    public synchronized static void initialize(MetricsConfig config) {
        STATS_PROVIDER.setProvider(config);
        DYNAMIC_LOGGER.setLogger(STATS_PROVIDER.createDynamicLogger());
    }

    public synchronized static StatsProvider getMetricsProvider() {
        return STATS_PROVIDER;
    }

    public synchronized static StatsLogger createStatsLogger(String loggerName) {
        return STATS_PROVIDER.createStatsLogger(loggerName);
    }

    public synchronized static DynamicLogger getDynamicLogger() {
        return DYNAMIC_LOGGER;
    }
}
