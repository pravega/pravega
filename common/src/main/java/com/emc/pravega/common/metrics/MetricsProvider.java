/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.metrics;

import com.codahale.metrics.MetricRegistry;


public class MetricsProvider {
    static final MetricRegistry YAMMERMETRICS = new MetricRegistry();
    private static final StatsProviderProxy STATS_PROVIDER;
    private static final DynamicLoggerProxy DYNAMIC_LOGGER;
    static {
        STATS_PROVIDER = new StatsProviderProxy();
        DYNAMIC_LOGGER = new DynamicLoggerProxy(STATS_PROVIDER.createDynamicLogger());
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
