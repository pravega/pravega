/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.service.server.logs;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.InvalidPropertyValueException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 * Durable Log Configuration.
 */
public class DurableLogConfig {
    //region Config Names
    public static final Property<Integer> CHECKPOINT_MIN_COMMIT_COUNT = Property.named("checkpointMinCommitCount", 300);
    public static final Property<Integer> CHECKPOINT_COMMIT_COUNT = Property.named("checkpointCommitCountThreshold", 300);
    public static final Property<Long> CHECKPOINT_TOTAL_COMMIT_LENGTH = Property.named("checkpointTotalCommitLengthThreshold", 256 * 1024 * 1024L);
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
