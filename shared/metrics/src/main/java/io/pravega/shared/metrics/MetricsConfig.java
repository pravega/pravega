/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    public final static Property<Boolean> ENABLE_STATISTICS = Property.named("statistics.enable", false, "enableStatistics");
    public final static Property<Integer> DYNAMIC_CACHE_SIZE = Property.named("dynamicCache.size", 100000, "dynamicCacheSize");
    public final static Property<Integer> DYNAMIC_CACHE_EVICTION_DURATION_MINUTES = Property.named("dynamicCache.eviction.duration.minutes", 3, "dynamicCacheEvictionDurationMinutes");
    public final static Property<Integer> OUTPUT_FREQUENCY = Property.named("output.frequency.seconds", 60, "outputFrequencySeconds");
    public final static Property<String> METRICS_PREFIX = Property.named("prefix", "pravega", "metricsPrefix");
    public final static Property<String> STATSD_HOST = Property.named("statsD.connect.host", "localhost", "statsDHost");
    public final static Property<Integer> STATSD_PORT = Property.named("statsD.connect.port", 8125, "statsDPort");
    public final static Property<String> INFLUXDB_URI = Property.named("influxDB.connect.uri", "http://localhost:8086", "influxDBURI");
    public final static Property<String> INFLUXDB_NAME = Property.named("influxDB.connect.db.name", "pravega", "influxDBName");
    public final static Property<String> INFLUXDB_USERNAME = Property.named("influxDB.connect.credentials.username", "", "influxDBUserName");
    public final static Property<String> INFLUXDB_PASSWORD = Property.named("influxDB.connect.credentials.pwd", "", "influxDBPassword");
    public final static Property<String> INFLUXDB_RETENTION_POLICY = Property.named("influxDB.retention", "", "influxDBRetention");
    public final static Property<Boolean> ENABLE_STATSD_REPORTER = Property.named("statsD.reporter.enable", false, "enableStatsDReporter");
    public final static Property<Boolean> ENABLE_INFLUXDB_REPORTER = Property.named("influxDB.reporter.enable", false, "enableInfluxDBReporter");
    public final static Property<Boolean> ENABLE_PROMETHEUS = Property.named("prometheus.enable", false);
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
    private final int dynamicCacheSize;

    /**
     * Cache eviction duration for dynamic metrics.
     */

    @Getter
    private Duration dynamicCacheEvictionDurationMinutes;

    /**
     * Gets a value indicating output frequency in seconds.
     */
    @Getter
    private final Duration outputFrequencySeconds;

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
    private final char[] influxDBPassword;

    /**
     * The retention policy of InfluxDB, e.g. "2h", "52w".
     */
    @Getter
    private final String influxDBRetention;

    /**
     * The status of enable StatsD reporter.
     */
    @Getter
    private final boolean enableStatsDReporter;

    /**
     * The status of enable InfluxDB reporter.
     */
    @Getter
    private final boolean enableInfluxDBReporter;

    /**
     * The status of enabling Prometheus.
     */
    @Getter
    private final boolean enablePrometheus;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MetricsConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private MetricsConfig(TypedProperties properties) throws ConfigurationException {
        this.enableStatistics = properties.getBoolean(ENABLE_STATISTICS);
        this.dynamicCacheSize = properties.getInt(DYNAMIC_CACHE_SIZE);
        this.dynamicCacheEvictionDurationMinutes = Duration.ofMinutes(properties.getInt(DYNAMIC_CACHE_EVICTION_DURATION_MINUTES));
        this.outputFrequencySeconds = Duration.ofSeconds(properties.getInt(OUTPUT_FREQUENCY));
        this.metricsPrefix = properties.get(METRICS_PREFIX);
        this.statsDHost = properties.get(STATSD_HOST);
        this.statsDPort = properties.getInt(STATSD_PORT);
        this.influxDBUri = properties.get(INFLUXDB_URI);
        this.influxDBName = properties.get(INFLUXDB_NAME);
        this.influxDBUserName = properties.get(INFLUXDB_USERNAME);
        this.influxDBPassword = properties.get(INFLUXDB_PASSWORD).toCharArray();
        this.influxDBRetention = properties.get(INFLUXDB_RETENTION_POLICY);
        this.enableInfluxDBReporter = properties.getBoolean(ENABLE_INFLUXDB_REPORTER);
        this.enableStatsDReporter = properties.getBoolean(ENABLE_STATSD_REPORTER);
        this.enablePrometheus = properties.getBoolean(ENABLE_PROMETHEUS);
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
