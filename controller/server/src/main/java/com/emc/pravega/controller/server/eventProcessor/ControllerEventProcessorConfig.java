/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.controller.eventProcessor.CheckpointConfig;
import com.emc.pravega.stream.ScalingPolicy;

/**
 * Configuration of controller event processors.
 */
public interface ControllerEventProcessorConfig {

    /**
     * Fetches the scope name of controller event processor streams.
     *
     * @return Scope name of controller event processor streams.
     */
    String getScopeName();

    /**
     * Fetches name of the commit stream.
     *
     * @return Name of the commit stream.
     */
    String getCommitStreamName();

    /**
     * Fetches commit stream scaling policy.
     *
     * @return Commit stream scaling policy.
     */
    ScalingPolicy getCommitStreamScalingPolicy();

    /**
     * Fetches name of the abort stream.
     *
     * @return Name of the abort stream.
     */
    String getAbortStreamName();

    /**
     * Fetches abort stream scaling policy.
     *
     * @return abort stream scaling policy.
     */

    ScalingPolicy getAbortStreamScalingPolicy();

    /**
     * Fetches name of the reader group processing events from commit stream.
     *
     * @return Name of the reader group processing events from commit stream.
     */
    String getCommitReaderGroupName();

    /**
     * Fetches the number of readers in a single controller instance participating in commit reader group.
     *
     * @return The number of readers in a single controller instance participating in commit reader group.
     */
    int getCommitReaderGroupSize();

    /**
     * Fetches name of the reader group processing events from abort stream.
     *
     * @return Name of the reader group processing events from abort stream.
     */
    String getAbortReaderGroupName();

    /**
     * Fetches the number of readers in a single controller instance participating in abort reader group.
     *
     * @return The number of readers in a single controller instance participating in abort reader group.
     */
    int getAbortReaderGroupSize();

    /**
     * Fetches checkpoint configuration for commit stream event processors.
     *
     * @return Checkpoint configuration for commit stream event processors.
     */
    CheckpointConfig getCommitCheckpointConfig();

    /**
     * Fetches checkpoint configuration for abort stream event processors.
     *
     * @return Checkpoint configuration for abort stream event processors.
     */
    CheckpointConfig getAbortCheckpointConfig();

    /**
     * Fetches name of the scale stream.
     *
     * @return Name of the scale stream.
     */
    String getScaleStreamName();

    /**
     * Fetches name of the reader group processing events from scale stream.
     *
     * @return Name of the reader group processing events from scale stream.
     */
    String getScaleReaderGroupName();

    /**
     * Fetches scale stream scaling policy.
     *
     * @return Scale stream scaling policy.
     */
    ScalingPolicy getScaleStreamScalingPolicy();

    /**
     * Fetches checkpoint configuration for scale stream event processors.
     *
     * @return Checkpoint configuration for scale stream event processors.
     */
    CheckpointConfig getScaleCheckpointConfig();
}
