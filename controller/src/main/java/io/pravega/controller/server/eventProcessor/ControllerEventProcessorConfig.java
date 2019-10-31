/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.client.stream.ScalingPolicy;

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
     * Fetches name of the request stream.
     *
     * @return Name of the request stream.
     */
    String getRequestStreamName();

    /**
     * Fetches name of the reader group processing events from request stream.
     *
     * @return Name of the reader group processing events from request stream.
     */
    String getRequestReaderGroupName();

    /**
     * Fetches request stream scaling policy.
     *
     * @return Request stream scaling policy.
     */
    ScalingPolicy getRequestStreamScalingPolicy();

    /**
     * Fetches checkpoint configuration for request stream event processors.
     *
     * @return Checkpoint configuration for request stream event processors.
     */
    CheckpointConfig getRequestStreamCheckpointConfig();
}
