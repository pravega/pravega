/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.server;

/**
 * Defines an immutable Stream Segment Container Metadata.
 */
public interface ContainerMetadata {
    /**
     * The initial Sequence Number. All operations will get sequence numbers starting from this value.
     */
    long INITIAL_OPERATION_SEQUENCE_NUMBER = 0;

    /**
     * Reserved value that indicates a missing StreamSegmentId. No valid StreamSegment can have this ID.
     */
    long NO_STREAM_SEGMENT_ID = Long.MIN_VALUE;

    /**
     * Gets a value indicating the Id of the StreamSegmentContainer this Metadata refers to.
     */
    int getContainerId();

    /**
     * Gets a value indicating whether we are currently in Recovery Mode.
     */
    boolean isRecoveryMode();

    /**
     * Gets a value indicating the current Operation Sequence Number.
     */
    long getOperationSequenceNumber();

    /**
     * Gets the Id of the StreamSegment with given name.
     *
     * @param streamSegmentName The case-sensitive StreamSegment name.
     * @return The Id of the StreamSegment, or NO_STREAM_SEGMENT_ID if the Metadata has no knowledge of it.
     */
    long getStreamSegmentId(String streamSegmentName);

    /**
     * Gets the StreamSegmentMetadata mapped to the given StreamSegment Id.
     *
     * @param streamSegmentId The Id of the StreamSegment to query for.
     * @return The mapped StreamSegmentMetadata, or null if none is.
     */
    SegmentMetadata getStreamSegmentMetadata(long streamSegmentId);
}
