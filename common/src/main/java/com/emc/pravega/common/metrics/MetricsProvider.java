/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.metrics;

import com.codahale.metrics.MetricRegistry;

public class MetricsProvider {
    public static final MetricRegistry YAMMERMETRICS = new MetricRegistry();
    private static final StatsProvider NULLPROVIDER = new NullStatsProvider();
    private static final StatsProvider YAMMERPROVIDER = new YammerStatsProvider();
    private static final MetricsProvider INSTANCE  = new MetricsProvider();

    // Dynamic logger
    private static final DynamicLogger YAMMERDYNAMICLOGGER = YAMMERPROVIDER.createDynamicLogger();
    private static final DynamicLogger NULLDYNAMICLOGGER = NULLPROVIDER.createDynamicLogger();

    private MetricsProvider() {
    }

    public static StatsProvider getMetricsProvider() {
        return  MetricsConfig.enableStatistics() ? YAMMERPROVIDER : NULLPROVIDER;
    }

    public static StatsLogger createStatsLogger(String loggerName) {
        return  MetricsConfig.enableStatistics() ?
                YAMMERPROVIDER.createStatsLogger(loggerName) :
                NULLPROVIDER.createStatsLogger(loggerName);
    }

    public static DynamicLogger getDynamicLogger() {
        return  MetricsConfig.enableStatistics() ?
                YAMMERDYNAMICLOGGER :
                NULLDYNAMICLOGGER;
    }
}
