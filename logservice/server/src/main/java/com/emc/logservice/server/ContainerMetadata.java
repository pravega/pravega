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

/**
 * Defines an immutable Stream Segment Container Metadata.
 */
public interface ContainerMetadata extends SegmentMetadataCollection {
    /**
     * Gets a value indicating the Id of the StreamSegmentContainer this Metadata refers to.
     *
     * @return
     */
    String getContainerId();

    /**
     * Gets a value indicating whether we are currently in Recovery Mode.
     *
     * @return
     */
    boolean isRecoveryMode();

    /**
     * Gets the next available Operation Sequence Number. Atomically increments the value by 1 with every call.
     *
     * @return The next available Operation Sequence Number.
     * @throws IllegalStateException If the Metadata is in Recovery Mode.
     */
    long getNewOperationSequenceNumber();
}
