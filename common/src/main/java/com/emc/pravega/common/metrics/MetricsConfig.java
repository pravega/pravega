/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.common.metrics;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.Property;
import com.emc.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 * General configuration for Metrics.
 */
public class MetricsConfig {
    //region Config Names
    public final static Property<Boolean> ENABLE_STATISTICS = new Property<>("enableStatistics", true);
    public final static Property<Long> DYNAMIC_CACHE_SIZE = new Property<>("dynamicCacheSize", 1000000L);
    public final static Property<Long> DYNAMIC_TTL_SECONDS = new Property<>("dynamicTTLSeconds", 120L);
    public final static Property<Integer> OUTPUT_FREQUENCY = new Property<>("yammerStatsOutputFrequencySeconds", 60);
    public final static Property<String> METRICS_PREFIX = new Property<>("yammerMetricsPrefix", "pravega");
    public final static Property<String> CSV_ENDPOINT = new Property<>("yammerCSVEndpoint", "/tmp/csv");
    public final static Property<String> STATSD_HOST = new Property<>("yammerStatsDHost", "localhost");
    public final static Property<Integer> STATSD_PORT = new Property<>("yammerStatsDPort", 8125);
    private static final String COMPONENT_CODE = "metrics";

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
        this.enableStatistics = properties.getBoolean(ENABLE_STATISTICS);
        this.dynamicCacheSize = properties.getLong(DYNAMIC_CACHE_SIZE);
        this.dynamicTTLSeconds = properties.getLong(DYNAMIC_TTL_SECONDS);
        this.statsOutputFrequencySeconds = properties.getInt(OUTPUT_FREQUENCY);
        this.metricsPrefix = properties.get(METRICS_PREFIX);
        this.csvEndpoint = properties.get(CSV_ENDPOINT);
        this.statsDHost = properties.get(STATSD_HOST);
        this.statsDPort = properties.getInt(STATSD_PORT);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<MetricsConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, MetricsConfig::new);
    }

    //endregion
}
