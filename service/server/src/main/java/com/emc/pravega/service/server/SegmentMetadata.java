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

package com.emc.pravega.service.server;

import com.emc.pravega.service.contracts.SegmentProperties;

/**
 * Defines an immutable StreamSegment Metadata.
 */
public interface SegmentMetadata extends SegmentProperties {
    /**
     * Defines an attribute value that denotes a missing value.
     */
    Long NULL_ATTRIBUTE_VALUE = Long.MIN_VALUE;

    /**
     * Gets a value indicating the id of this StreamSegment.
     */
    long getId();

    /**
     * Gets a value indicating the id of this StreamSegment's parent.
     */
    long getParentId();

    /**
     * Gets a value indicating the id of the Container this StreamSegment belongs to.
     */
    int getContainerId();

    /**
     * Gets a value indicating whether this StreamSegment has been merged into another.
     */
    boolean isMerged();

    /**
     * Gets a value indicating whether this StreamSegment has been sealed in Storage.
     * This is different from isSealed(), which returns true if the StreamSegment has been sealed in DurableLog or in Storage.
     */
    boolean isSealedInStorage();

    /**
     * Gets a value indicating the length of this StreamSegment for the part that exists in Storage Only.
     */
    long getStorageLength();

    /**
     * Gets a value indicating the length of this entire StreamSegment (the part in Storage + the part in DurableLog).
     */
    long getDurableLogLength();
}
