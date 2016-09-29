/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.config;

import java.util.Properties;

import com.emc.pravega.common.util.ComponentConfig;
import com.emc.pravega.common.util.InvalidPropertyValueException;
import com.emc.pravega.common.util.MissingPropertyException;

/**
 * Durable Log Configuration.
 */
public class DurableLogConfig extends ComponentConfig {
    //region Members
    public final static String COMPONENT_CODE = "durablelog";
    public static final String PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT = "checkpointMinCommitCount";
    public static final String PROPERTY_CHECKPOINT_COMMIT_COUNT = "checkpointCommitCountThreshold";
    public static final String PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH = "checkpointTotalCommitLengthThreshold";

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
     * @throws MissingPropertyException Whenever a required Property is missing from the given properties collection.
     * @throws NumberFormatException    Whenever a Property has a value that is invalid for it.
     * @throws NullPointerException     If any of the arguments are null.
     * @throws IllegalArgumentException If componentCode is an empty string..
     */
    public DurableLogConfig(Properties properties) throws MissingPropertyException, InvalidPropertyValueException {
        super(properties, COMPONENT_CODE);
    }

    //endregion

    /**
     * Gets a value indicating the minimum number of commits that need to be accumulated in order to trigger a Checkpoint.
     *
     * @return
     */
    public int getCheckpointMinCommitCount() {
        return this.checkpointMinCommitCount;
    }

    /**
     * Gets a value indicating the number of commits that would trigger a Checkpoint.
     *
     * @return
     */
    public int getCheckpointCommitCountThreshold() {
        return this.checkpointCommitCountThreshold;
    }

    /**
     * Gets a value indicating the number of bytes appended that would trigger a Checkpoint.
     *
     * @return
     */
    public long getCheckpointTotalCommitLengthThreshold() {
        return this.checkpointTotalCommitLengthThreshold;
    }

    @Override
    protected void refresh() throws MissingPropertyException, InvalidPropertyValueException {
        this.checkpointMinCommitCount = getInt32Property(PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, DEFAULT_MIN_COMMIT_COUNT);
        this.checkpointCommitCountThreshold = getInt32Property(PROPERTY_CHECKPOINT_COMMIT_COUNT, DEFAULT_COMMIT_COUNT);
        if (this.checkpointMinCommitCount > this.checkpointCommitCountThreshold) {
            throw new InvalidPropertyValueException(String.format("Property '%s' (%d) cannot be larger than Property '%s' (%d).", PROPERTY_CHECKPOINT_MIN_COMMIT_COUNT, this.checkpointMinCommitCount, PROPERTY_CHECKPOINT_COMMIT_COUNT, this.checkpointCommitCountThreshold));
        }

        this.checkpointTotalCommitLengthThreshold = getInt64Property(PROPERTY_CHECKPOINT_TOTAL_COMMIT_LENGTH, DEFAULT_TOTAL_COMMIT_LENGTH);
    }
}
