/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.common.metrics;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;

import java.util.Properties;

/**
 * General configuration for Metrics.
 */
public class MetricsConfig extends ComponentConfig {
    //region Members
    public static final String COMPONENT_CODE = "metrics";
    public final static String ENABLE_STATISTICS = "enableStatistics";
    public final static String DYNAMIC_CACHE_SIZE = "dynamicCacheSize";
    public final static String DYNAMIC_TTL_SECONDS = "dynamicTTLSeconds";
    public final static String OUTPUT_FREQUENCY = "yammerStatsOutputFrequencySeconds";
    public final static String METRICS_PREFIX = "yammerMetricsPrefix";
    public final static String CSV_ENDPOINT = "yammerCSVEndpoint";
    public final static String STATSD_HOST = "yammerStatsDHost";
    public final static String STATSD_PORT = "yammerStatsDPort";

    public final static boolean DEFAULT_ENABLE_STATISTICS = true;
    public final static long DEFAULT_DYNAMIC_CACHE_SIZE = 1000000L;
    public final static long DEFAULT_DYNAMIC_TTL_SECONDS = 120L;
    public final static int DEFAULT_OUTPUT_FREQUENCY = 60;
    public final static String DEFAULT_METRICS_PREFIX = "pravega";
    public final static String DEFAULT_CSV_ENDPOINT = "/tmp/csv";
    public final static String DEFAULT_STATSD_HOST = "localhost";
    public final static int DEFAULT_STATSD_PORT = 8125;

    private static boolean enableStatistics = true;
    private static long dynamicCacheSize = 1000000L;
    private static long dynamicTTLSeconds = 120L;
    private int yammerStatsOutputFrequencySeconds;
    private String yammerMetricsPrefix;
    private String yammerCSVEndpoint;
    private String yammerStatsDHost;
    private int yammerStatsDPort;

    /**
     * Creates a new instance of the MetricsConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws ConfigurationException   When a configuration issue has been detected. This can be:
     *                                  MissingPropertyException (a required Property is missing from the given properties collection),
     *                                  NumberFormatException (a Property has a value that is invalid for it).
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If componentCode is an empty string..
     */
    public MetricsConfig(Properties properties) throws ConfigurationException {
        super(properties, COMPONENT_CODE);
    }

    /**
     * Gets a value indicating the status of enable statistics.
     */
    public static boolean enableStatistics() {
        return enableStatistics;
    }

    /**
     * Gets cache size for dynamic metrics.
     */
    public static long getDynamicCacheSize() {
        return dynamicCacheSize;
    }

    /**
     * Gets cache TTL for dynamic metrics.
     */
    public static long getDynamicTTLSeconds() {
        return dynamicTTLSeconds;
    }

    /**
     * Gets a value indicating output frequency in seconds.
     */
    public int getStatsOutputFrequency() {
        return this.yammerStatsOutputFrequencySeconds;
    }

    /**
     * Gets a value indicating the metrics prefix.
     */
    public String getMetricsPrefix() {
        return this.yammerMetricsPrefix;
    }

    /**
     * Gets a value indicating the metrics csv endpoint.
     */
    public String getCSVEndpoint() {
        return this.yammerCSVEndpoint;
    }

    /**
     * Gets a value indicating the host name (no port) where StatsD is listening.
     */
    public String getStatsDHost() {
        return this.yammerStatsDHost;
    }

    /**
     * Gets a value indicating the port where StatsD is listening.
     */
    public int getStatsDPort() {
        return this.yammerStatsDPort;
    }


    @Override
    public void refresh() throws ConfigurationException {
        this.enableStatistics = getBooleanProperty(ENABLE_STATISTICS, DEFAULT_ENABLE_STATISTICS);
        this.dynamicCacheSize = getInt64Property(DYNAMIC_CACHE_SIZE, DEFAULT_DYNAMIC_CACHE_SIZE);
        this.dynamicTTLSeconds = getInt64Property(DYNAMIC_TTL_SECONDS, DEFAULT_DYNAMIC_TTL_SECONDS);
        this.yammerStatsOutputFrequencySeconds = getInt32Property(OUTPUT_FREQUENCY, DEFAULT_OUTPUT_FREQUENCY);
        this.yammerMetricsPrefix = getProperty(METRICS_PREFIX, DEFAULT_METRICS_PREFIX);
        this.yammerCSVEndpoint = getProperty(CSV_ENDPOINT, DEFAULT_CSV_ENDPOINT);
        this.yammerStatsDHost = getProperty(STATSD_HOST, DEFAULT_STATSD_HOST);
        this.yammerStatsDPort = getInt32Property(STATSD_PORT, DEFAULT_STATSD_PORT);
    }

}
