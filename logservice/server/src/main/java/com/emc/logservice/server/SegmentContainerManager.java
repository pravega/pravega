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

package com.emc.logservice.server;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Manages the lifecycle of all SegmentContainers within a SegmentContainerRegistry.
 * Implementations of this interface will connect to cluster organizers (i.e. ZooKeeper) and control the SegmentContainers
 * within a given Registry based on events from such entities.
 */
public interface SegmentContainerManager extends AutoCloseable {
    /**
     * Initializes the SegmentContainerManager.
     *
     * @param timeout Timeout for the operation.
     * @return A CompletableFuture that, when completed, indicates that this operation completed. If the operation failed,
     * the Future will contain the Exception that caused the failure.
     */
    CompletableFuture<Void> initialize(Duration timeout);

    @Override
    void close();
}
