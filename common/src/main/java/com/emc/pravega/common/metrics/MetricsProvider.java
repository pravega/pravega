/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.metrics;

import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicReference;

public class MetricsProvider {
    public static final MetricRegistry YAMMERMETRICS = new MetricRegistry();
    private static final AtomicReference<StatsProviderProxy> STATS_PROVIDER;
    private static final AtomicReference<DynamicLoggerProxy> DYNAMIC_LOGGER;
    static {
        STATS_PROVIDER = new AtomicReference<>(new StatsProviderProxy());
        DYNAMIC_LOGGER = new AtomicReference<>(new DynamicLoggerProxy(STATS_PROVIDER.get().createDynamicLogger()));
    }

    public synchronized static void initialize(MetricsConfig config) {
        STATS_PROVIDER.get().setProvider(config);
        DYNAMIC_LOGGER.get().setLogger(STATS_PROVIDER.get().createDynamicLogger());
    }

    public synchronized static StatsProvider getMetricsProvider() {
        return STATS_PROVIDER.get();
    }

    public synchronized static StatsLogger createStatsLogger(String loggerName) {
        return STATS_PROVIDER.get().createStatsLogger(loggerName);
    }

    public synchronized static DynamicLogger getDynamicLogger() {
        return DYNAMIC_LOGGER.get();
    }
}
