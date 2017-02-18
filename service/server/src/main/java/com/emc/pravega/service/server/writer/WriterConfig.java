/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.writer;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;

import java.time.Duration;
import java.util.Properties;

/**
 * Writer Configuration.
 */
public class WriterConfig extends ComponentConfig {
    //region Members

    public final static String COMPONENT_CODE = "writer";
    public static final String PROPERTY_FLUSH_THRESHOLD_BYTES = "flushThresholdBytes";
    public static final String PROPERTY_FLUSH_THRESHOLD_MILLIS = "flushThresholdMillis";
    public static final String PROPERTY_MAX_FLUSH_SIZE_BYTES = "maxFlushSizeBytes";
    public static final String PROPERTY_MAX_ITEMS_TO_READ_AT_ONCE = "maxItemsToReadAtOnce";
    public static final String PROPERTY_MIN_READ_TIMEOUT_MILLIS = "minReadTimeoutMillis";
    public static final String PROPERTY_MAX_READ_TIMEOUT_MILLIS = "maxReadTimeoutMillis";
    public static final String PROPERTY_ERROR_SLEEP_MILLIS = "errorSleepMillis";
    public static final String PROPERTY_FLUSH_TIMEOUT_MILLIS = "flushTimeoutMillis";
    public static final String PROPERTY_ACK_TIMEOUT_MILLIS = "ackTimeoutMillis";
    public static final String PROPERTY_SHUTDOWN_TIMEOUT_MILLIS = "shutdownTimeoutMillis";

    private static final int DEFAULT_FLUSH_THRESHOLD_BYTES = 4 * 1024 * 1024; // 4MB
    private static final int DEFAULT_FLUSH_THRESHOLD_MILLIS = 30 * 1000; // 30s
    private static final int DEFAULT_MAX_FLUSH_SIZE_BYTES = DEFAULT_FLUSH_THRESHOLD_BYTES;
    private static final int DEFAULT_MAX_ITEMS_TO_READ_AT_ONCE = 1000;
    private static final int DEFAULT_MIN_READ_TIMEOUT_MILLIS = 2 * 1000; // 2s
    private static final int DEFAULT_MAX_READ_TIMEOUT_MILLIS = 30 * 60 * 1000; // 30 min
    private static final int DEFAULT_ERROR_SLEEP_MILLIS = 1000; // 1 s
    private static final int DEFAULT_FLUSH_TIMEOUT_MILLIS = 60 * 1000; // 60s
    private static final int DEFAULT_ACK_TIMEOUT_MILLIS = 15 * 1000; // 15s
    private static final int DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 10 * 1000; // 10s

    private int flushThresholdBytes;
    private Duration flushThresholdTime;
    private int maxFlushSizeBytes;
    private int maxItemsToReadAtOnce;
    private Duration minReadTimeout;
    private Duration maxReadTimeout;
    private Duration errorSleepDuration;
    private Duration flushTimeout;
    private Duration ackTimeout;
    private Duration shutdownTimeout;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the WriterConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws ConfigurationException   When a configuration issue has been detected. This can be:
     *                                  MissingPropertyException (a required Property is missing from the given properties collection),
     *                                  NumberFormatException (a Property has a value that is invalid for it).
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If componentCode is an empty string..
     */
    public WriterConfig(Properties properties) throws ConfigurationException {
        super(properties, COMPONENT_CODE);
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating the minimum number of bytes to wait for before flushing aggregated data for a Segment to Storage.
     */
    public int getFlushThresholdBytes() {
        return this.flushThresholdBytes;
    }

    /**
     * Gets a value indicating the minimum amount of time to wait for before flushing aggregated data for a Segment to Storage.
     */
    public Duration getFlushThresholdTime() {
        return this.flushThresholdTime;
    }

    /**
     * Gets a value indicating the maximum number of bytes that can be flushed with a single write operation.
     */
    public int getMaxFlushSizeBytes() {
        return this.maxFlushSizeBytes;
    }

    /**
     * Gets a value indicating the maximum number of items to read every time a read is issued to the OperationLog.
     */
    public int getMaxItemsToReadAtOnce() {
        return this.maxItemsToReadAtOnce;
    }

    /**
     * Gets a value indicating the minimum timeout to supply to the WriterDataSource.read() method.
     */
    public Duration getMinReadTimeout() {
        return this.minReadTimeout;
    }

    /**
     * Gets a value indicating the maximum timeout to supply to the WriterDataSource.read() method.
     */
    public Duration getMaxReadTimeout() {
        return this.maxReadTimeout;
    }

    /**
     * Gets a value indicating the amount of time to sleep if an iteration error was detected.
     */
    public Duration getErrorSleepDuration() {
        return this.errorSleepDuration;
    }

    /**
     * Gets a value indicating the timeout for the Flush Stage.
     */
    public Duration getFlushTimeout() {
        return this.flushTimeout;
    }

    /**
     * Gets a value indicating the timeout for the Shutdown operation (how much to wait for the current iteration to end
     * before giving up).
     */
    public Duration getShutdownTimeout() {
        return this.shutdownTimeout;
    }

    /**
     * Gets a value indicating the timeout for the Ack Stage.
     */
    public Duration getAckTimeout() {
        return this.ackTimeout;
    }

    //endregion

    //region ComponentConfig Implementation

    @Override
    protected void refresh() throws ConfigurationException {
        this.flushThresholdBytes = getInt32Property(PROPERTY_FLUSH_THRESHOLD_BYTES, DEFAULT_FLUSH_THRESHOLD_BYTES);
        if (this.flushThresholdBytes < 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a non-negative integer.", PROPERTY_FLUSH_THRESHOLD_BYTES));
        }

        long flushThresholdMillis = getInt64Property(PROPERTY_FLUSH_THRESHOLD_MILLIS, DEFAULT_FLUSH_THRESHOLD_MILLIS);
        this.flushThresholdTime = Duration.ofMillis(flushThresholdMillis);

        this.maxFlushSizeBytes = getInt32Property(PROPERTY_MAX_FLUSH_SIZE_BYTES, DEFAULT_MAX_FLUSH_SIZE_BYTES);

        this.maxItemsToReadAtOnce = getInt32Property(PROPERTY_MAX_ITEMS_TO_READ_AT_ONCE, DEFAULT_MAX_ITEMS_TO_READ_AT_ONCE);
        if (this.maxItemsToReadAtOnce <= 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer.", PROPERTY_MAX_ITEMS_TO_READ_AT_ONCE));
        }

        long minReadTimeoutMillis = getInt64Property(PROPERTY_MIN_READ_TIMEOUT_MILLIS, DEFAULT_MIN_READ_TIMEOUT_MILLIS);
        long maxReadTimeoutMillis = getInt64Property(PROPERTY_MAX_READ_TIMEOUT_MILLIS, DEFAULT_MAX_READ_TIMEOUT_MILLIS);
        if (minReadTimeoutMillis < 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer.", PROPERTY_MIN_READ_TIMEOUT_MILLIS));
        }

        if (minReadTimeoutMillis > maxReadTimeoutMillis) {
            throw new ConfigurationException(String.format("Property '%s' must be smaller than or equal to '%s'.", PROPERTY_MIN_READ_TIMEOUT_MILLIS, PROPERTY_MAX_READ_TIMEOUT_MILLIS));
        }

        this.minReadTimeout = Duration.ofMillis(minReadTimeoutMillis);
        this.maxReadTimeout = Duration.ofMillis(maxReadTimeoutMillis);
        long errorSleepMillis = getInt64Property(PROPERTY_ERROR_SLEEP_MILLIS, DEFAULT_ERROR_SLEEP_MILLIS);
        this.errorSleepDuration = Duration.ofMillis(errorSleepMillis);

        long flushTimeoutMillis = getInt64Property(PROPERTY_FLUSH_TIMEOUT_MILLIS, DEFAULT_FLUSH_TIMEOUT_MILLIS);
        this.flushTimeout = Duration.ofMillis(flushTimeoutMillis);

        long ackTimeoutMillis = getInt64Property(PROPERTY_ACK_TIMEOUT_MILLIS, DEFAULT_ACK_TIMEOUT_MILLIS);
        this.ackTimeout = Duration.ofMillis(ackTimeoutMillis);

        long shutdownTimeoutMillis = getInt64Property(PROPERTY_SHUTDOWN_TIMEOUT_MILLIS, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
        this.shutdownTimeout = Duration.ofMillis(shutdownTimeoutMillis);
    }

    //endregion
}
