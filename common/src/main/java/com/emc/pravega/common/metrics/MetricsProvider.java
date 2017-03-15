/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.metrics;

import com.codahale.metrics.MetricRegistry;

public class MetricsProvider {
    public static final MetricRegistry YAMMERMETRICS = new MetricRegistry();
    private static final MetricsProvider INSTANCE  = new MetricsProvider();
    
    private final StatsProvider nullProvider = new NullStatsProvider();
    private final StatsProvider yammerProvider;

    // Dynamic logger
    private final DynamicLogger yammerDynamicLogger;
    private final DynamicLogger nullDynamicLogger;
    private MetricsConfig metricsConfig;

    private MetricsProvider() {
        this.metricsConfig = MetricsConfig.builder().build();
        this.nullDynamicLogger = nullProvider.createDynamicLogger();
        this.yammerProvider = new YammerStatsProvider(metricsConfig);
        this.yammerDynamicLogger = yammerProvider.createDynamicLogger();
    }

    public static StatsProvider getMetricsProvider() {
        return INSTANCE.metricsConfig.enableStatistics() ? INSTANCE.yammerProvider : INSTANCE.nullProvider;
    }

    public static StatsLogger createStatsLogger(String loggerName) {
        return INSTANCE.metricsConfig.enableStatistics() ? INSTANCE.yammerProvider.createStatsLogger(loggerName)
                : INSTANCE.nullProvider.createStatsLogger(loggerName);
    }

    public static DynamicLogger getDynamicLogger() {
        return INSTANCE.metricsConfig.enableStatistics() ? INSTANCE.yammerDynamicLogger : INSTANCE.nullDynamicLogger;
    }
}
