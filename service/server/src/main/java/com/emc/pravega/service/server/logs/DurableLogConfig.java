/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.logs;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.InvalidPropertyValueException;
import com.emc.pravega.common.util.Property;
import com.emc.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 * Durable Log Configuration.
 */
public class DurableLogConfig {
    //region Config Names
    public static final Property<Integer> CHECKPOINT_MIN_COMMIT_COUNT = Property.named("checkpointMinCommitCount", 10);
    public static final Property<Integer> CHECKPOINT_COMMIT_COUNT = Property.named("checkpointCommitCountThreshold", Integer.MAX_VALUE);
    public static final Property<Long> CHECKPOINT_TOTAL_COMMIT_LENGTH = Property.named("checkpointTotalCommitLengthThreshold", Long.MAX_VALUE);
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
