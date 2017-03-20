/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.metrics;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;

import java.util.Properties;

import lombok.extern.slf4j.Slf4j;

/**
 * General configuration for Metrics.
 */
@Slf4j
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
    public final static String GRAPHITE_HOST = "yammerGraphiteHost";
    public final static String GRAPHITE_PORT = "yammerGraphitePort";
    public final static String JMX_DOMAIN = "yammerJMXDomain";
    public final static String GANGLIA_HOST = "yammerGangliaHost";
    public final static String GANGLIA_PORT = "yammerGangliaPort";
    public final static String ENABLE_CONSOLE_REPORTER = "enableConsoleReporter";

    public final static boolean DEFAULT_ENABLE_STATISTICS = true;
    public final static long DEFAULT_DYNAMIC_CACHE_SIZE = 1000000L;
    public final static long DEFAULT_DYNAMIC_TTL_SECONDS = 120L;
    public final static int DEFAULT_OUTPUT_FREQUENCY = 60;
    public final static String DEFAULT_METRICS_PREFIX = "pravega";
    public final static String DEFAULT_CSV_ENDPOINT = "/tmp/csv";
    public final static String DEFAULT_STATSD_HOST = "localhost";
    public final static int DEFAULT_STATSD_PORT = 8125;
    public final static String DEFAULT_GRAPHITE_HOST = null;
    public final static int DEFAULT_GRAPHITE_PORT = 2003;
    public final static String DEFAULT_JMX_DOMAIN = null;
    public final static String DEFAULT_GANGLIA_HOST = null;
    public final static int DEFAULT_GANGLIA_PORT = 8649;
    public final static boolean DEFAULT_ENABLE_CONSOLE_REPORTER = false;

    private boolean enableStatistics = true;
    private long dynamicCacheSize = 1000000L;
    private long dynamicTTLSeconds = 120L;
    private int yammerStatsOutputFrequencySeconds;
    private String yammerMetricsPrefix;
    private String yammerCSVEndpoint;
    private String yammerStatsDHost;
    private int yammerStatsDPort;
    private String yammerGraphiteHost;
    private int yammerGraphitePort;
    private String yammerJMXDomain;
    private String yammerGangliaHost;
    private int yammerGangliaPort;
    private boolean enableConsoleReporter = false;

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
        refresh();
    }

    /**
     * Gets a value indicating the status of enable statistics.
     */
    public boolean enableStatistics() {
        return enableStatistics;
    }

    /**
     * Gets cache size for dynamic metrics.
     */
    public long getDynamicCacheSize() {
        return dynamicCacheSize;
    }

    /**
     * Gets cache TTL for dynamic metrics.
     */
    public long getDynamicTTLSeconds() {
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

    /**
     * Gets a value indicating the host name (no port) where Graphite is listening.
     */
    public String getGraphiteHost() {
        return this.yammerGraphiteHost;
    }

    /**
     * Gets a value indicating the port where Graphite is listening.
     */
    public int getGraphitePort() {
        return this.yammerGraphitePort;
    }

    /**
     * Gets a value indicating the JMX Domain.
     */
    public String getJMXDomain() {
        return this.yammerJMXDomain;
    }

    /**
     * Gets a value indicating the host name (no port) of Ganglia.
     */
    public String getGangliaHost() {
        return this.yammerGangliaHost;
    }

    /**
     * Gets a value indicating the port of Ganglia.
     */
    public int getGangliaPort() {
        return this.yammerGangliaPort;
    }

    /**
     * Gets a value indicating the status of enable console reporter.
     */
    public boolean enableConsoleReporter() {
        return enableConsoleReporter;
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
        this.yammerGraphiteHost = getProperty(GRAPHITE_HOST, DEFAULT_GRAPHITE_HOST);
        this.yammerGraphitePort = getInt32Property(GRAPHITE_PORT, DEFAULT_GRAPHITE_PORT);
        this.yammerJMXDomain = getProperty(JMX_DOMAIN, DEFAULT_JMX_DOMAIN);
        this.yammerGangliaHost = getProperty(GANGLIA_HOST, DEFAULT_GANGLIA_HOST);
        this.yammerGangliaPort = getInt32Property(GANGLIA_PORT, DEFAULT_GANGLIA_PORT);
        this.enableConsoleReporter = getBooleanProperty(ENABLE_CONSOLE_REPORTER, DEFAULT_ENABLE_CONSOLE_REPORTER);
    }

}
