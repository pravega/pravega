/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.metrics;

import com.codahale.metrics.MetricRegistry;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsProvider {
    public static final AtomicReference<MetricRegistry> YAMMERMETRICS;
    private static final AtomicReference<MetricsProvider> INSTANCE;
    static {
        Properties properties = new Properties();

        properties.setProperty("metrics." + MetricsConfig.ENABLE_STATISTICS, "false");
        YAMMERMETRICS = new AtomicReference<>(new MetricRegistry());
        INSTANCE = new AtomicReference<>(new MetricsProvider(new MetricsConfig(properties)));
    }

    // Dynamic logger
    private final StatsProvider statsProvider;
    private final DynamicLogger dynamicLogger;
    private MetricsConfig metricsConfig;

    private MetricsProvider(MetricsConfig config) {
        this.metricsConfig = config;

        if (config.enableStatistics()) {
            log.info("Enabling Yammer provider");
            YAMMERMETRICS.set(new MetricRegistry());
            this.statsProvider = new YammerStatsProvider(metricsConfig);
        } else {
            log.info("Enabling null provider");
            this.statsProvider = new NullStatsProvider();
        }
        this.dynamicLogger = this.statsProvider.createDynamicLogger();
    }

    public static synchronized void initialize(MetricsConfig metricsConfig) {
        INSTANCE.get().statsProvider.close();
        INSTANCE.set(new MetricsProvider(metricsConfig));
    }

    public static StatsProvider getStatsProvider() {
        MetricsProvider metricsProvider = INSTANCE.get();

        return metricsProvider.statsProvider;
    }

    public static synchronized StatsLogger createStatsLogger(String loggerName) {
        MetricsProvider metricsProvider = INSTANCE.get();

        return metricsProvider.statsProvider.createStatsLogger(loggerName);
    }

    public static DynamicLogger getDynamicLogger() {
        MetricsProvider metricsProvider = INSTANCE.get();

        return  metricsProvider.dynamicLogger;
    }

    public static MetricsConfig getConfig() {
        return INSTANCE.get().metricsConfig;
    }
}
