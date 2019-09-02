/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.writer;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import java.time.Duration;

import lombok.Getter;

/**
 * Writer Configuration.
 */
public class WriterConfig {
    //region Config Names

    public static final Property<Integer> FLUSH_THRESHOLD_BYTES = Property.named("flushThresholdBytes", 4 * 1024 * 1024);
    public static final Property<Long> FLUSH_THRESHOLD_MILLIS = Property.named("flushThresholdMillis", 30 * 1000L);
    public static final Property<Integer> MAX_FLUSH_SIZE_BYTES = Property.named("maxFlushSizeBytes", FLUSH_THRESHOLD_BYTES.getDefaultValue());
    public static final Property<Integer> MAX_ITEMS_TO_READ_AT_ONCE = Property.named("maxItemsToReadAtOnce", 1000);
    public static final Property<Long> MIN_READ_TIMEOUT_MILLIS = Property.named("minReadTimeoutMillis", 2 * 1000L);
    public static final Property<Long> MAX_READ_TIMEOUT_MILLIS = Property.named("maxReadTimeoutMillis", 30 * 60 * 1000L);
    public static final Property<Long> ERROR_SLEEP_MILLIS = Property.named("errorSleepMillis", 1000L);
    public static final Property<Long> FLUSH_TIMEOUT_MILLIS = Property.named("flushTimeoutMillis", 60 * 1000L);
    public static final Property<Long> ACK_TIMEOUT_MILLIS = Property.named("ackTimeoutMillis", 15 * 1000L);
    public static final Property<Long> SHUTDOWN_TIMEOUT_MILLIS = Property.named("shutdownTimeoutMillis", 10 * 1000L);
    private static final String COMPONENT_CODE = "writer";

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
        this.flushThresholdBytes = properties.getInt(FLUSH_THRESHOLD_BYTES);
        if (this.flushThresholdBytes < 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a non-negative integer.", FLUSH_THRESHOLD_BYTES));
        }

        this.flushThresholdTime = Duration.ofMillis(properties.getLong(FLUSH_THRESHOLD_MILLIS));
        this.maxFlushSizeBytes = properties.getInt(MAX_FLUSH_SIZE_BYTES);
        this.maxItemsToReadAtOnce = properties.getInt(MAX_ITEMS_TO_READ_AT_ONCE);
        if (this.maxItemsToReadAtOnce <= 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer.", MAX_ITEMS_TO_READ_AT_ONCE));
        }

        long minReadTimeoutMillis = properties.getLong(MIN_READ_TIMEOUT_MILLIS);
        long maxReadTimeoutMillis = properties.getLong(MAX_READ_TIMEOUT_MILLIS);
        if (minReadTimeoutMillis < 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer.", MIN_READ_TIMEOUT_MILLIS));
        }

        if (minReadTimeoutMillis > maxReadTimeoutMillis) {
            throw new ConfigurationException(String.format("Property '%s' must be smaller than or equal to '%s'.", MIN_READ_TIMEOUT_MILLIS, MAX_READ_TIMEOUT_MILLIS));
        }

        this.minReadTimeout = Duration.ofMillis(minReadTimeoutMillis);
        this.maxReadTimeout = Duration.ofMillis(maxReadTimeoutMillis);
        this.errorSleepDuration = Duration.ofMillis(properties.getLong(ERROR_SLEEP_MILLIS));
        this.flushTimeout = Duration.ofMillis(properties.getLong(FLUSH_TIMEOUT_MILLIS));
        this.ackTimeout = Duration.ofMillis(properties.getLong(ACK_TIMEOUT_MILLIS));
        this.shutdownTimeout = Duration.ofMillis(properties.getLong(SHUTDOWN_TIMEOUT_MILLIS));
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<WriterConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, WriterConfig::new);
    }

    //endregion
}
