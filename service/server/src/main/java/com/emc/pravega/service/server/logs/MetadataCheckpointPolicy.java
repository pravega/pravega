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

package com.emc.pravega.service.server.logs;

import com.google.common.base.Preconditions;

import java.util.concurrent.Executor;

/**
 * Configurable Checkpointing Policy for the Metadata for a single container.
 */
public class MetadataCheckpointPolicy {
    // region Members

    private final DurableLogConfig config;
    private final Runnable createCheckpointCallback;
    private final Executor executor;
    private int commitCount;
    private long accumulatedLength;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the MetadataCheckpointPolicy class.
     *
     * @param config                   The DurableLogConfig to use.
     * @param createCheckpointCallback A callback to invoke when a checkpoint needs to be created.
     * @param executor                 An Executor to use to invoke the createCheckpointCallback.
     */
    public MetadataCheckpointPolicy(DurableLogConfig config, Runnable createCheckpointCallback, Executor executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(createCheckpointCallback, "createCheckpointCallback");
        Preconditions.checkNotNull(executor, "executor");

        this.config = config;
        this.createCheckpointCallback = createCheckpointCallback;
        this.executor = executor;
        this.commitCount = 0;
        this.accumulatedLength = 0;
    }

    //endregion

    //region Operations

    /**
     * Records that an operation with the given data length has been processed.
     *
     * @param commitLength The length of the commit (i.e., DataFrame).
     */
    public synchronized void recordCommit(int commitLength) {
        Preconditions.checkArgument(commitLength >= 0, "commitLength must be a non-negative number.");
        // Update counters.
        this.commitCount++;
        this.accumulatedLength += commitLength;

        int minCount = this.config.getCheckpointMinCommitCount();
        int countThreshold = this.config.getCheckpointCommitCountThreshold();
        long lengthThreshold = this.config.getCheckpointTotalCommitLengthThreshold();
        if (this.commitCount >= minCount && (this.commitCount >= countThreshold || this.accumulatedLength >=
                lengthThreshold)) {
            // Reset counters.
            this.commitCount = 0;
            this.accumulatedLength = 0;

            // Invoke callback.
            this.executor.execute(this.createCheckpointCallback);
        }
    }

    @Override
    public String toString() {
        return String.format("Count = %d/%d, Length = %d/%d", this.commitCount,
                this.config.getCheckpointCommitCountThreshold(), this.accumulatedLength,
                this.config.getCheckpointTotalCommitLengthThreshold());
    }

    //endregion
}
