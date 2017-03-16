/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.logs;

import com.emc.pravega.common.util.ConfigBuilder;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.InvalidPropertyValueException;
import com.emc.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 * Durable Log Configuration.
 */
public class DurableLogConfig {
    //region Config Names
    public static final String PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT = "checkpointMinCommitCount";
    public static final String PROPERTY_CHECKPOINT_COMMIT_COUNT = "checkpointCommitCountThreshold";
    public static final String PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH = "checkpointTotalCommitLengthThreshold";
    private static final String COMPONENT_CODE = "durablelog";

    private final static int DEFAULT_MIN_COMMIT_COUNT = 10;
    private final static int DEFAULT_COMMIT_COUNT = Integer.MAX_VALUE;
    private final static long DEFAULT_TOTAL_COMMIT_LENGTH = Long.MAX_VALUE;

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
        this.checkpointMinCommitCount = properties.getInt32(PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, DEFAULT_MIN_COMMIT_COUNT);
        this.checkpointCommitCountThreshold = properties.getInt32(PROPERTY_CHECKPOINT_COMMIT_COUNT, DEFAULT_COMMIT_COUNT);
        if (this.checkpointMinCommitCount > this.checkpointCommitCountThreshold) {
            throw new InvalidPropertyValueException(String.format("Property '%s' (%d) cannot be larger than Property '%s' (%d).",
                    PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, this.checkpointMinCommitCount,
                    PROPERTY_CHECKPOINT_COMMIT_COUNT, this.checkpointCommitCountThreshold));
        }

        this.checkpointTotalCommitLengthThreshold = properties.getInt64(PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH, DEFAULT_TOTAL_COMMIT_LENGTH);
    }

    /**
     * Creates a Builder that can be used to programmatically create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<DurableLogConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, DurableLogConfig::new);
    }

    //endregion
}
