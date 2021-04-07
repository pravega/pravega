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
package io.pravega.segmentstore.server;

import java.util.Collection;

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
     * @return The Id of the StreamSegmentContainer.
     */
    int getContainerId();

    /**
     * Gets a value indicating the current Container Epoch.
     * <p>
     * An Epoch is a monotonically strictly number that changes (not necessarily incremented) every time the Container
     * is successfully recovered. This usually corresponds to a successful exclusive lock acquisition of the DurableDataLog
     * corresponding to this Container, thus fencing out any other existing instances of this Container holding that lock.
     * <p>
     * For example, if Container X instance A has an epoch smaller than that of Container X instance B, then it is safe
     * to assume that B was recovered later than A and A should be in the process of shutting down and not respond to any
     * requests or make any further modifications.
     *
     * @return The Epoch of the current Container instance.
     */
    long getContainerEpoch();

    /**
     * Gets a value indicating whether we are currently in Recovery Mode.
     * @return true if currently in Recovery Mode, false otherwise.
     */
    boolean isRecoveryMode();

    /**
     * Gets a value indicating the current Operation Sequence Number.
     * @return The Current Operation Sequence Number.
     */
    long getOperationSequenceNumber();

    /**
     * Gets the Id of the StreamSegment with given name.
     *
     * @param streamSegmentName The case-sensitive StreamSegment name.
     * @param updateLastUsed    If true, marks the given segment as 'touched' in the metadata stats, which are used for
     *                          determining segment metadata evictions.
     * @return The Id of the StreamSegment, or NO_STREAM_SEGMENT_ID if the Metadata has no knowledge of it.
     */
    long getStreamSegmentId(String streamSegmentName, boolean updateLastUsed);

    /**
     * Gets the StreamSegmentMetadata mapped to the given StreamSegment Id.
     *
     * @param streamSegmentId The Id of the StreamSegment to query for.
     * @return The mapped StreamSegmentMetadata, or null if none is.
     */
    SegmentMetadata getStreamSegmentMetadata(long streamSegmentId);

    /**
     * Gets a collection containing all StreamSegmentIds currently mapped.
     * @return The collection containing all StreamSegmentIds currently mapped.
     */
    Collection<Long> getAllStreamSegmentIds();

    /**
     * Gets a value indicating the maximum number of segments that can be registered in this metadata at any given time.
     *
     * @return The maximum number of segments.
     */
    int getMaximumActiveSegmentCount();

    /**
     * Gets a value indicating the current number of registered segments.
     *
     * @return The count.
     */
    int getActiveSegmentCount();
}
