/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.common.metrics;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 * General configuration for Metrics.
 */
public class MetricsConfig {
    //region Config Names
    public final static String ENABLE_STATISTICS = "enableStatistics";
    public final static String DYNAMIC_CACHE_SIZE = "dynamicCacheSize";
    public final static String DYNAMIC_TTL_SECONDS = "dynamicTTLSeconds";
    public final static String OUTPUT_FREQUENCY = "yammerStatsOutputFrequencySeconds";
    public final static String METRICS_PREFIX = "yammerMetricsPrefix";
    public final static String CSV_ENDPOINT = "yammerCSVEndpoint";
    public final static String STATSD_HOST = "yammerStatsDHost";
    public final static String STATSD_PORT = "yammerStatsDPort";
    private static final String COMPONENT_CODE = "metrics";

    private final static boolean DEFAULT_ENABLE_STATISTICS = true;
    private final static long DEFAULT_DYNAMIC_CACHE_SIZE = 1000000L;
    private final static long DEFAULT_DYNAMIC_TTL_SECONDS = 120L;
    private final static int DEFAULT_OUTPUT_FREQUENCY = 60;
    private final static String DEFAULT_METRICS_PREFIX = "pravega";
    private final static String DEFAULT_CSV_ENDPOINT = "/tmp/csv";
    private final static String DEFAULT_STATSD_HOST = "localhost";
    private final static int DEFAULT_STATSD_PORT = 8125;

    //endregion

    //region Members

    /**
     * The status of enable statistics.
     */
    @Getter
    private final boolean enableStatistics;

    /**
     * Cache size for dynamic metrics.
     */
    @Getter
    private final long dynamicCacheSize;

    /**
     * Cache TTL for dynamic metrics.
     */
    @Getter
    private final long dynamicTTLSeconds;

    /**
     * Gets a value indicating output frequency in seconds.
     */
    @Getter
    private final int statsOutputFrequencySeconds;

    /**
     * The metrics prefix.
     */
    @Getter
    private final String metricsPrefix;

    /**
     * The metrics csv endpoint.
     */
    @Getter
    private final String csvEndpoint;

    /**
     * The host name (no port) where StatsD is listening.
     */
    @Getter
    private final String statsDHost;

    /**
     * The port where StatsD is listening.
     */
    @Getter
    private final int statsDPort;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MetricsConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private MetricsConfig(TypedProperties properties) throws ConfigurationException {
        this.enableStatistics = properties.getBoolean(ENABLE_STATISTICS, DEFAULT_ENABLE_STATISTICS);
        this.dynamicCacheSize = properties.getInt64(DYNAMIC_CACHE_SIZE, DEFAULT_DYNAMIC_CACHE_SIZE);
        this.dynamicTTLSeconds = properties.getInt64(DYNAMIC_TTL_SECONDS, DEFAULT_DYNAMIC_TTL_SECONDS);
        this.statsOutputFrequencySeconds = properties.getInt32(OUTPUT_FREQUENCY, DEFAULT_OUTPUT_FREQUENCY);
        this.metricsPrefix = properties.get(METRICS_PREFIX, DEFAULT_METRICS_PREFIX);
        this.csvEndpoint = properties.get(CSV_ENDPOINT, DEFAULT_CSV_ENDPOINT);
        this.statsDHost = properties.get(STATSD_HOST, DEFAULT_STATSD_HOST);
        this.statsDPort = properties.getInt32(STATSD_PORT, DEFAULT_STATSD_PORT);
    }

    /**
     * Creates a Builder that can be used to programmatically create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<MetricsConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, MetricsConfig::new);
    }

    //endregion
}
