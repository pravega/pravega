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

    public static final Property<Integer> FLUSH_THRESHOLD_BYTES = Property.named("flush.threshold.bytes", 4 * 1024 * 1024, "flushThresholdBytes");
    public static final Property<Long> FLUSH_THRESHOLD_MILLIS = Property.named("flush.threshold.milliseconds", 30 * 1000L, "flushThresholdMillis");
    public static final Property<Integer> FLUSH_ATTRIBUTES_THRESHOLD = Property.named("flush.attributes.threshold", 200, "flushAttributesThreshold");
    public static final Property<Integer> MAX_FLUSH_SIZE_BYTES = Property.named("flush.size.bytes.max", FLUSH_THRESHOLD_BYTES.getDefaultValue(), "maxFlushSizeBytes");
    public static final Property<Integer> MAX_ITEMS_TO_READ_AT_ONCE = Property.named("itemsToReadAtOnce.max", 1000, "maxItemsToReadAtOnce");
    public static final Property<Long> MIN_READ_TIMEOUT_MILLIS = Property.named("read.timeout.milliseconds.min", 2 * 1000L, "minReadTimeoutMillis");
    public static final Property<Long> MAX_READ_TIMEOUT_MILLIS = Property.named("read.timeout.milliseconds.max", 30 * 60 * 1000L, "maxReadTimeoutMillis");
    public static final Property<Long> ERROR_SLEEP_MILLIS = Property.named("error.sleep.milliseconds", 1000L, "errorSleepMillis");
    public static final Property<Long> FLUSH_TIMEOUT_MILLIS = Property.named("flush.timeout.milliseconds", 60 * 1000L, "flushTimeoutMillis");
    public static final Property<Long> ACK_TIMEOUT_MILLIS = Property.named("ack.timeout.milliseconds", 15 * 1000L, "ackTimeoutMillis");
    public static final Property<Long> SHUTDOWN_TIMEOUT_MILLIS = Property.named("shutDown.timeout.milliseconds", 10 * 1000L, "shutdownTimeoutMillis");
    public static final Property<Long> MAX_ROLLOVER_SIZE = Property.named("rollover.size.bytes.max", 134217728L, "maxRolloverSizeBytes");
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
     * The minimum number of attributes that should accumulate before flushing them into the Attribute Index.
     */
    @Getter
    private final int flushAttributesThreshold;

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

    /**
     * The maximum Rolling Size (in bytes) for a Segment in Storage. This will preempt any value configured on the Segment
     * via the Segment's Attributes (no rolling size may exceed this value).
     */
    @Getter
    private final long maxRolloverSize;

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
        this.flushAttributesThreshold = properties.getInt(FLUSH_ATTRIBUTES_THRESHOLD);
        if (this.flushAttributesThreshold < 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a non-negative integer.", FLUSH_THRESHOLD_BYTES));
        }

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
        this.maxRolloverSize = Math.max(0, properties.getLong(MAX_ROLLOVER_SIZE));
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
