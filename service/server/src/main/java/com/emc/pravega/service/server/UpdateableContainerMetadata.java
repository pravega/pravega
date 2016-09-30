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

import java.util.Collection;

/**
 * Defines an updateable StreamSegment Metadata.
 */
public interface UpdateableContainerMetadata extends ContainerMetadata, RecoverableMetadata, TruncationMarkerRepository {
    /**
     * Maps a new StreamSegment Name to its assigned Id.
     *
     * @param streamSegmentName The case-sensitive name of the StreamSegment to map.
     * @param streamSegmentId   The Id of the StreamSegment.
     * @return An UpdateableSegmentMetadata that represents the metadata for the newly mapped StreamSegment.
     */
    UpdateableSegmentMetadata mapStreamSegmentId(String streamSegmentName, long streamSegmentId);

    /**
     * Maps a new StreamSegment to its Parent StreamSegment.
     * This is used for batches that are dependent on their parent StreamSegments.
     *
     * @param streamSegmentName     The case-sensitive name of the StreamSegment to map.
     * @param streamSegmentId       The Id of the StreamSegment to map.
     * @param parentStreamSegmentId The Id of the Parent StreamSegment.
     * @return An UpdateableSegmentMetadata that represents the metadata for the newly mapped StreamSegment.
     * @throws IllegalArgumentException If the parentStreamSegmentId refers to an unknown StreamSegment.
     */
    UpdateableSegmentMetadata mapStreamSegmentId(String streamSegmentName, long streamSegmentId, long parentStreamSegmentId);

    /**
     * Gets a collection containing all StreamSegmentIds currently mapped.
     *
     * @return
     */
    Collection<Long> getAllStreamSegmentIds();

    /**
     * Marks the StreamSegment and all child StreamSegments as deleted.
     *
     * @param streamSegmentName The name of the StreamSegment to delete.
     * @return A Collection of StreamSegment names that have been deleted. This includes the given StreamSegmentName,
     * as well as the names of any child StreamSegments that have been deleted.
     */
    Collection<String> deleteStreamSegment(String streamSegmentName);

    /**
     * Gets the next available Operation Sequence Number. Atomically increments the value by 1 with every call.
     *
     * @return The next available Operation Sequence Number.
     * @throws IllegalStateException If the Metadata is in Recovery Mode.
     */
    long nextOperationSequenceNumber();

    /**
     * Sets the current Operation Sequence Number.
     *
     * @param value The new Operation Sequence Number.
     * @throws IllegalStateException    If the Metadata is not in Recovery Mode.
     * @throws IllegalArgumentException If the new Sequence Number is not greater than the previous one.
     */
    void setOperationSequenceNumber(long value);

    /**
     * Gets the StreamSegmentMetadata mapped to the given StreamSegment Id.
     *
     * @param streamSegmentId The Id of the StreamSegment to query for.
     * @return The mapped StreamSegmentMetadata, or null if none is.
     */
    UpdateableSegmentMetadata getStreamSegmentMetadata(long streamSegmentId);
}
