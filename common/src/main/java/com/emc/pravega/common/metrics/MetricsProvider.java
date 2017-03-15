/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.metrics;

import com.codahale.metrics.MetricRegistry;

import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsProvider {
    public static final MetricRegistry YAMMERMETRICS = new MetricRegistry();
    private static final AtomicReference<MetricsProvider> INSTANCE  = new AtomicReference<>();
    
    private final StatsProvider nullProvider = new NullStatsProvider();
    private final StatsProvider yammerProvider;

    // Dynamic logger
    private final DynamicLogger yammerDynamicLogger;
    private final DynamicLogger nullDynamicLogger;
    private MetricsConfig metricsConfig;

    private MetricsProvider(MetricsConfig config) {
        this.metricsConfig = config;
        this.nullDynamicLogger = nullProvider.createDynamicLogger();
        this.yammerProvider = new YammerStatsProvider(metricsConfig);
        this.yammerDynamicLogger = yammerProvider.createDynamicLogger();
    }

    public static void initialize(MetricsConfig metricsConfig) {
        Preconditions.checkArgument(INSTANCE.get() == null, "MetricsProvider has already been initialized");
        INSTANCE.set(new MetricsProvider(metricsConfig));
    }

    public static StatsProvider getMetricsProvider() {
        Preconditions.checkNotNull(INSTANCE.get(), "MetricsProvider not initialized");

        MetricsProvider metricsProvider = INSTANCE.get();

        return metricsProvider.metricsConfig.enableStatistics() ? metricsProvider.yammerProvider
                : metricsProvider.nullProvider;
    }

    public static StatsLogger createStatsLogger(String loggerName) {
        Preconditions.checkNotNull(INSTANCE.get(), "MetricsProvider not initialized");

        MetricsProvider metricsProvider = INSTANCE.get();

        return metricsProvider.metricsConfig.enableStatistics() ? metricsProvider.yammerProvider.createStatsLogger(loggerName)
                : metricsProvider.nullProvider.createStatsLogger(loggerName);
    }

    public static DynamicLogger getDynamicLogger() {
        Preconditions.checkNotNull(INSTANCE.get(), "MetricsProvider not initialized");

        MetricsProvider metricsProvider = INSTANCE.get();

        return metricsProvider.metricsConfig.enableStatistics() ? metricsProvider.yammerDynamicLogger
                : metricsProvider.nullDynamicLogger;
    }

    public static MetricsConfig getConfig() {
        Preconditions.checkNotNull(INSTANCE.get(), "MetricsProvider not initialized");

        return INSTANCE.get().metricsConfig;
    }
}
