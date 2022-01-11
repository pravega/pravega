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
package io.pravega.segmentstore.server.logs;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.InvalidPropertyValueException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import java.time.Duration;
import lombok.Getter;

/**
 * Durable Log Configuration.
 */
public class DurableLogConfig {
    //region Config Names

    public static final Property<Integer> CHECKPOINT_MIN_COMMIT_COUNT = Property.named("checkpoint.commit.count.min", 300, "checkpointMinCommitCount");
    public static final Property<Integer> CHECKPOINT_COMMIT_COUNT = Property.named("checkpoint.commit.threshold.count", 300, "checkpointCommitCountThreshold");
    public static final Property<Long> CHECKPOINT_TOTAL_COMMIT_LENGTH = Property.named("checkpoint.commit.length.total", 256 * 1024 * 1024L, "checkpointTotalCommitLengthThreshold");
    public static final Property<Integer> START_RETRY_DELAY_MILLIS = Property.named("start.retry.delay.millis", 60 * 1000, "startRetryDelayMillis");
    public static final Property<Integer> MAX_BATCHING_DELAY_MILLIS = Property.named("throttler.max.batching.delay.millis", 50);
    public static final Property<Integer> MAX_DELAY_MILLIS = Property.named("throttler.max.delay.millis", 25000);
    public static final Property<Integer> OPERATION_LOG_TARGET_SIZE = Property.named("throttler.operation.log.size.target", (int) (1_000_000 * 0.95));
    public static final Property<Integer> OPERATION_LOG_MAX_SIZE = Property.named("throttler.operation.log.size.max", 1_000_000);
    private static final String COMPONENT_CODE = "durablelog";

    //endregion

    //region Members

    /**
     * The minimum number of commits that need to be accumulated in order to trigger a Checkpoint.
     */
    @Getter
    private final int checkpointMinCommitCount;

    /**
     * The number of commits that would trigger a Checkpoint.
     */
    @Getter
    private final int checkpointCommitCountThreshold;

    /**
     * The number of bytes appended that would trigger a Checkpoint.
     */
    @Getter
    private final long checkpointTotalCommitLengthThreshold;

    /**
     * The amount of time to wait between consecutive start attempts in case of retryable startup failure (i.e., offline).
     */
    @Getter
    private final Duration startRetryDelay;

    /**
     * Maximum delay (millis) we are willing to introduce in order to perform batching.
     */
    @Getter
    private final int maxBatchingDelayMillis;

    /**
     * Maximum delay (millis) we are willing to introduce in order to throttle the incoming operations.
     */
    @Getter
    private final int maxDelayMillis;

    /**
     * Maximum size (in number of operations) of the OperationLog, above which maximum throttling will be applied.
     */
    @Getter
    private final int operationLogMaxSize;

    /**
     * Desired size (in number of operations) of the OperationLog, above which a gradual throttling will begin.
     */
    @Getter
    private final int operationLogTargetSize;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DurableLogConfig class.
     *
     * @param properties The TypedProperties object to read Properties from.
     */
    private DurableLogConfig(TypedProperties properties) throws ConfigurationException {
        this.checkpointMinCommitCount = properties.getInt(CHECKPOINT_MIN_COMMIT_COUNT);
        this.checkpointCommitCountThreshold = properties.getInt(CHECKPOINT_COMMIT_COUNT);
        if (this.checkpointMinCommitCount > this.checkpointCommitCountThreshold) {
            throw new InvalidPropertyValueException(String.format("Property '%s' (%d) cannot be larger than Property '%s' (%d).",
                    CHECKPOINT_MIN_COMMIT_COUNT, this.checkpointMinCommitCount,
                    CHECKPOINT_COMMIT_COUNT, this.checkpointCommitCountThreshold));
        }

        this.checkpointTotalCommitLengthThreshold = properties.getLong(CHECKPOINT_TOTAL_COMMIT_LENGTH);
        int startRetryDelayMillis = properties.getInt(START_RETRY_DELAY_MILLIS);
        if (startRetryDelayMillis <= 0) {
            throw new ConfigurationException(String.format("Property '%s' must be a positive integer.", START_RETRY_DELAY_MILLIS));
        }
        this.startRetryDelay = Duration.ofMillis(startRetryDelayMillis);

        // Throttler configuration.
        this.maxBatchingDelayMillis = properties.getPositiveInt(MAX_BATCHING_DELAY_MILLIS);
        this.maxDelayMillis = properties.getPositiveInt(MAX_DELAY_MILLIS);
        this.operationLogMaxSize = properties.getPositiveInt(OPERATION_LOG_MAX_SIZE);
        this.operationLogTargetSize = properties.getPositiveInt(OPERATION_LOG_TARGET_SIZE);
        if (this.operationLogTargetSize >= this.operationLogMaxSize) {
            throw new ConfigurationException(String.format("Property '%s' ('%d') must be a smaller than Property '%s' ('%d').",
                    OPERATION_LOG_TARGET_SIZE, this.operationLogTargetSize,
                    OPERATION_LOG_MAX_SIZE, this.operationLogMaxSize));
        }
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<DurableLogConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, DurableLogConfig::new);
    }

    //endregion
}
