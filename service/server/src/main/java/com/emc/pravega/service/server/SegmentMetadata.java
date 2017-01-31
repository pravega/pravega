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

import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.SegmentProperties;

import java.util.Collection;
import java.util.UUID;

/**
 * Defines an immutable StreamSegment Metadata.
 */
public interface SegmentMetadata extends SegmentProperties {
    /**
     * Gets a value indicating the id of this StreamSegment.
     * @return stream segment Id.
     */
    long getId();

    /**
     * Gets a value indicating the id of this StreamSegment's parent.
     * @return parent stream segment Id.
     */
    long getParentId();

    /**
     * Gets a value indicating the id of the Container this StreamSegment belongs to.
     * @return container Id that this segment belongs to.
     */
    int getContainerId();

    /**
     * Gets a value indicating whether this StreamSegment has been merged into another.
     * @return whether this segment has merged into another or not.
     */
    boolean isMerged();

    /**
     * Gets a value indicating whether this StreamSegment has been sealed in Storage.
     * This is different from isSealed(), which returns true if the StreamSegment has been sealed in DurableLog or in Storage.
     * @return whether this segment has been sealed in Storage or not.
     */
    boolean isSealedInStorage();

    /**
     * Gets a value indicating the length of this StreamSegment for the part that exists in Storage Only.
     * @return the length of segment that exists in Storage only.
     */
    long getStorageLength();

    /**
     * Gets a value indicating the length of this entire StreamSegment (the part in Storage + the part in DurableLog).
     * @return the length of entire segment.
     */
    long getDurableLogLength();

    /**
     * Gets the Append Context for the Last Committed Append related to the given client.
     * Note that this may not be available for appends that occurred long in the past (this data is not persisted with
     * the metadata).
     *
     * @param clientId The Client Id to inquire for.
     * @return last append context with given client Id.
     */
    AppendContext getLastAppendContext(UUID clientId);

    /**
     * Gets a collection of all known Client Ids (mapped to AppendContexts).
     * @return known client Ids hat have appended to this segment
     */
    Collection<UUID> getKnownClientIds();
}
