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
package io.pravega.controller.server.eventProcessor;

import io.pravega.client.stream.ScalingPolicy;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import java.time.Duration;

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
    
    /**
     * Fetches rebalance interval set for event processors.
     *
     * @return period in milliseconds.
     */
    long getRebalanceIntervalMillis();

    /**
     * Fetches name of the request stream.
     *
     * @return Name of the request stream.
     */
    String getKvtStreamName();

    /**
     * Fetches name of the reader group processing events from request stream.
     *
     * @return Name of the reader group processing events from request stream.
     */
    String getKvtReaderGroupName();

    /**
     * Fetches scaling policy for stream used to store kvtable requests.
     *
     * @return Request kvtable scaling policy.
     */
    ScalingPolicy getKvtStreamScalingPolicy();

    /**
     * Gets a value indicating the amount of time to await a shutdown.
     *
     * @return Shutdown timeout.
     */
    Duration getShutdownTimeout();
}
