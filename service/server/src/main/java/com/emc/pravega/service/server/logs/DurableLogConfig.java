/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.logs;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.InvalidPropertyValueException;
import java.util.Properties;

/**
 * Durable Log Configuration.
 */
public class DurableLogConfig extends ComponentConfig {
    //region Members
    public static final String PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT = "checkpointMinCommitCount";
    public static final String PROPERTY_CHECKPOINT_COMMIT_COUNT = "checkpointCommitCountThreshold";
    public static final String PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH = "checkpointTotalCommitLengthThreshold";
    private static final String COMPONENT_CODE = "durablelog";

    private final static int DEFAULT_MIN_COMMIT_COUNT = 10;
    private final static int DEFAULT_COMMIT_COUNT = Integer.MAX_VALUE;
    private final static long DEFAULT_TOTAL_COMMIT_LENGTH = Long.MAX_VALUE;

    private int checkpointMinCommitCount;
    private int checkpointCommitCountThreshold;
    private long checkpointTotalCommitLengthThreshold;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the DurableLogConfig class.
     *
     * @param properties The java.util.Properties object to read Properties from.
     * @throws ConfigurationException   When a configuration issue has been detected. This can be:
     *                                  MissingPropertyException (a required Property is missing from the given properties collection),
     *                                  NumberFormatException (a Property has a value that is invalid for it).
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If componentCode is an empty string.
     */
    public DurableLogConfig(Properties properties) throws ConfigurationException {
        super(properties, COMPONENT_CODE);
    }

    /**
     * Creates a Builder that can be used to programmatically create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static Builder<DurableLogConfig> builder() {
        return ComponentConfig.builder(DurableLogConfig.class, COMPONENT_CODE);
    }

    //endregion

    /**
     * Gets a value indicating the minimum number of commits that need to be accumulated in order to trigger a Checkpoint.
     */
    public int getCheckpointMinCommitCount() {
        return this.checkpointMinCommitCount;
    }

    /**
     * Gets a value indicating the number of commits that would trigger a Checkpoint.
     */
    public int getCheckpointCommitCountThreshold() {
        return this.checkpointCommitCountThreshold;
    }

    /**
     * Gets a value indicating the number of bytes appended that would trigger a Checkpoint.
     */
    public long getCheckpointTotalCommitLengthThreshold() {
        return this.checkpointTotalCommitLengthThreshold;
    }

    @Override
    protected void refresh() throws ConfigurationException {
        this.checkpointMinCommitCount = getInt32Property(PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, DEFAULT_MIN_COMMIT_COUNT);
        this.checkpointCommitCountThreshold = getInt32Property(PROPERTY_CHECKPOINT_COMMIT_COUNT, DEFAULT_COMMIT_COUNT);
        if (this.checkpointMinCommitCount > this.checkpointCommitCountThreshold) {
            throw new InvalidPropertyValueException(String.format("Property '%s' (%d) cannot be larger than Property '%s' (%d).", PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, this.checkpointMinCommitCount, PROPERTY_CHECKPOINT_COMMIT_COUNT, this.checkpointCommitCountThreshold));
        }

        this.checkpointTotalCommitLengthThreshold = getInt64Property(PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH, DEFAULT_TOTAL_COMMIT_LENGTH);
    }
}
