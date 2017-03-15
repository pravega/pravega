/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.metrics;

import com.codahale.metrics.MetricRegistry;

import java.util.Properties;
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

    private static void initializeDefault() {
        Properties properties = new Properties();

        properties.setProperty("metrics.enableStatistics", "false");
        INSTANCE.set(new MetricsProvider(new MetricsConfig(properties)));
    }

    public static StatsProvider getMetricsProvider() {
        if (INSTANCE.get() == null) {
            initializeDefault();
        }

        MetricsProvider metricsProvider = INSTANCE.get();

        return metricsProvider.metricsConfig.enableStatistics() ? metricsProvider.yammerProvider
                : metricsProvider.nullProvider;
    }

    public static StatsLogger createStatsLogger(String loggerName) {
        if (INSTANCE.get() == null) {
            initializeDefault();
        }

        MetricsProvider metricsProvider = INSTANCE.get();

        return metricsProvider.metricsConfig.enableStatistics() ? metricsProvider.yammerProvider.createStatsLogger(loggerName)
                : metricsProvider.nullProvider.createStatsLogger(loggerName);
    }

    public static DynamicLogger getDynamicLogger() {
        if (INSTANCE.get() == null) {
            initializeDefault();
        }

        MetricsProvider metricsProvider = INSTANCE.get();

        return metricsProvider.metricsConfig.enableStatistics() ? metricsProvider.yammerDynamicLogger
                : metricsProvider.nullDynamicLogger;
    }

    public static MetricsConfig getConfig() {
        if (INSTANCE.get() == null) {
            initializeDefault();
        }

        return INSTANCE.get().metricsConfig;
    }
}
