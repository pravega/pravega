/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.writer;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.TypedProperties;
import java.time.Duration;
import lombok.Getter;

/**
 * Writer Configuration.
 */
public class WriterConfig {
    //region Config Names

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
    private static final String COMPONENT_CODE = "writer";

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

    //endregion

    //region Members

    /**
     * The minimum number of bytes to wait for before flushing aggregated data for a Segment to Storage.
     */
    @Getter
    private final int flushThresholdBytes;

    /**
     * The minimum amount of time to wait for before flushing aggregated data for a Segment to Storage.
     */
    @Getter
    private final Duration flushThresholdTime;

    /**
     * The maximum number of bytes that can be flushed with a single write operation.
     */
    @Getter
    private final int maxFlushSizeBytes;

    /**
     * The maximum number of items to read every time a read is issued to the OperationLog.
     */
    @Getter
    private final int maxItemsToReadAtOnce;

    /**
     * The minimum timeout to supply to the WriterDataSource.read() method.
     */
    @Getter
    private final Duration minReadTimeout;

    /**
     * The maximum timeout to supply to the WriterDataSource.read() method.
     */
    @Getter
    private final Duration maxReadTimeout;

    /**
     * The amount of time to sleep if an iteration error was detected.
     */
    @Getter
    private final Duration errorSleepDuration;

    /**
     * The timeout for the Flush Stage.
     */
    @Getter
    private final Duration flushTimeout;

    /**
     * Gets a value indicating the timeout for the Ack Stage.
     */
    @Getter
    private final Duration ackTimeout;

    /**
     * The timeout for the Shutdown operation (how much to wait for the current iteration to end before giving up).
     */
    @Getter
    private final Duration shutdownTimeout;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the WriterConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private WriterConfig(TypedProperties properties) throws ConfigurationException {
        this.flushThresholdBytes = properties.getInt32(PROPERTY_FLUSH_THRESHOLD_BYTES, DEFAULT_FLUSH_THRESHOLD_BYTES);
        if (this.flushThresholdBytes < 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a non-negative integer.", PROPERTY_FLUSH_THRESHOLD_BYTES));
        }

        long flushThresholdMillis = properties.getInt64(PROPERTY_FLUSH_THRESHOLD_MILLIS, DEFAULT_FLUSH_THRESHOLD_MILLIS);
        this.flushThresholdTime = Duration.ofMillis(flushThresholdMillis);

        this.maxFlushSizeBytes = properties.getInt32(PROPERTY_MAX_FLUSH_SIZE_BYTES, DEFAULT_MAX_FLUSH_SIZE_BYTES);

        this.maxItemsToReadAtOnce = properties.getInt32(PROPERTY_MAX_ITEMS_TO_READ_AT_ONCE, DEFAULT_MAX_ITEMS_TO_READ_AT_ONCE);
        if (this.maxItemsToReadAtOnce <= 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer.", PROPERTY_MAX_ITEMS_TO_READ_AT_ONCE));
        }

        long minReadTimeoutMillis = properties.getInt64(PROPERTY_MIN_READ_TIMEOUT_MILLIS, DEFAULT_MIN_READ_TIMEOUT_MILLIS);
        long maxReadTimeoutMillis = properties.getInt64(PROPERTY_MAX_READ_TIMEOUT_MILLIS, DEFAULT_MAX_READ_TIMEOUT_MILLIS);
        if (minReadTimeoutMillis < 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer.", PROPERTY_MIN_READ_TIMEOUT_MILLIS));
        }

        if (minReadTimeoutMillis > maxReadTimeoutMillis) {
            throw new ConfigurationException(String.format("Property '%s' must be smaller than or equal to '%s'.", PROPERTY_MIN_READ_TIMEOUT_MILLIS, PROPERTY_MAX_READ_TIMEOUT_MILLIS));
        }

        this.minReadTimeout = Duration.ofMillis(minReadTimeoutMillis);
        this.maxReadTimeout = Duration.ofMillis(maxReadTimeoutMillis);
        long errorSleepMillis = properties.getInt64(PROPERTY_ERROR_SLEEP_MILLIS, DEFAULT_ERROR_SLEEP_MILLIS);
        this.errorSleepDuration = Duration.ofMillis(errorSleepMillis);

        long flushTimeoutMillis = properties.getInt64(PROPERTY_FLUSH_TIMEOUT_MILLIS, DEFAULT_FLUSH_TIMEOUT_MILLIS);
        this.flushTimeout = Duration.ofMillis(flushTimeoutMillis);

        long ackTimeoutMillis = properties.getInt64(PROPERTY_ACK_TIMEOUT_MILLIS, DEFAULT_ACK_TIMEOUT_MILLIS);
        this.ackTimeout = Duration.ofMillis(ackTimeoutMillis);

        long shutdownTimeoutMillis = properties.getInt64(PROPERTY_SHUTDOWN_TIMEOUT_MILLIS, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
        this.shutdownTimeout = Duration.ofMillis(shutdownTimeoutMillis);
    }

    /**
     * Creates a Builder that can be used to programmatically create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<WriterConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, WriterConfig::new);
    }

    //endregion
}
