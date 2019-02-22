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

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;
import java.time.Duration;

/**
 * General configuration for Metrics.
 */
public class MetricsConfig {
    //region Config Names
    public final static Property<Boolean> ENABLE_STATISTICS = Property.named("enableStatistics", true);
    public final static Property<Long> DYNAMIC_CACHE_SIZE = Property.named("dynamicCacheSize", 10000000L);
    public final static Property<Integer> DYNAMIC_CACHE_EVICTION_DURATION_MINUTES = Property.named("dynamicCacheEvictionDurationMinutes", 30);
    public final static Property<Integer> OUTPUT_FREQUENCY = Property.named("statsOutputFrequencySeconds", 60);
    public final static Property<String> METRICS_PREFIX = Property.named("metricsPrefix", "pravega");
    public final static Property<String> STATSD_HOST = Property.named("statsDHost", "localhost");
    public final static Property<Integer> STATSD_PORT = Property.named("statsDPort", 8125);
    public final static Property<String> INFLUXDB_URI = Property.named("influxDBURI", "http://localhost:8086");
    public final static Property<String> INFLUXDB_NAME = Property.named("influxDBName", "pravega");
    public final static Property<String> INFLUXDB_USERNAME = Property.named("influxDBUserName", "");
    public final static Property<String> INFLUXDB_PASSWORD = Property.named("influxDBPassword", "");
    public final static Property<Boolean> ENABLE_STATSD_REPORTER = Property.named("enableStatsdReporter", true);
    public final static Property<Boolean> ENABLE_INFLUXDB_REPORTER = Property.named("enableInfluxDBReporter", false);
    public static final String COMPONENT_CODE = "metrics";

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
     * Cache eviction duration for dynamic metrics.
     */

    @Getter
    private Duration dynamicCacheEvictionDurationMinutes;

    /**
     * Gets a value indicating output frequency in seconds.
     */
    @Getter
    private final Duration statsOutputFrequencySeconds;

    /**
     * The metrics prefix.
     */
    @Getter
    private final String metricsPrefix;

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

    /**
     * The URI of InfluxDB endpoint.
     */
    @Getter
    private final String influxDBUri;

    /**
     * The name of InfluxDB.
     */
    @Getter
    private final String influxDBName;

    /**
     * The username to access InfluxDB.
     */
    @Getter
    private final String influxDBUserName;

    /**
     * The password of user account accessing InfluxDB.
     */
    @Getter
    private final String influxDBPassword;

    /**
     * The status of enable StatsD reporter.
     */
    @Getter
    private final boolean enableStatsdReporter;

    /**
     * The status of enable InfluxDB reporter.
     */
    @Getter
    private final boolean enableInfluxDBReporter;

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
        this.dynamicCacheEvictionDurationMinutes = Duration.ofMinutes(properties.getInt(DYNAMIC_CACHE_EVICTION_DURATION_MINUTES));
        this.statsOutputFrequencySeconds = Duration.ofSeconds(properties.getInt(OUTPUT_FREQUENCY));
        this.metricsPrefix = properties.get(METRICS_PREFIX);
        this.statsDHost = properties.get(STATSD_HOST);
        this.statsDPort = properties.getInt(STATSD_PORT);
        this.influxDBUri = properties.get(INFLUXDB_URI);
        this.influxDBName = properties.get(INFLUXDB_NAME);
        this.influxDBUserName = properties.get(INFLUXDB_USERNAME);
        this.influxDBPassword = properties.get(INFLUXDB_PASSWORD);
        this.enableInfluxDBReporter = properties.getBoolean(ENABLE_INFLUXDB_REPORTER);
        this.enableStatsdReporter = properties.getBoolean(ENABLE_STATSD_REPORTER);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<MetricsConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, MetricsConfig::new);
    }


    @VisibleForTesting
    public void setDynamicCacheEvictionDuration(Duration duration) {
        this.dynamicCacheEvictionDurationMinutes = duration;
    }

    //endregion
}
