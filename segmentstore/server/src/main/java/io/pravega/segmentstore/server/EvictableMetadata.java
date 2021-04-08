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
 * Defines a ContainerMetadata that allows eviction of SegmentMetadatas.
 */
public interface EvictableMetadata extends ContainerMetadata {
    /**
     * Gets a collection of SegmentMetadata referring to Segments that are currently eligible for removal.
     *
     * @param sequenceNumberCutoff A Sequence Number that indicates the cutoff threshold. A Segment is eligible for eviction
     *                             if it has a LastUsed value smaller than this threshold.
     * @param maxCount             The maximum number of eviction candidates to return.
     * @return The collection of SegmentMetadata that can be cleaned up.
     */
    Collection<SegmentMetadata> getEvictionCandidates(long sequenceNumberCutoff, int maxCount);

    /**
     * Evicts the StreamSegments that match the given SegmentMetadata, but only if they are still eligible for removal.
     *
     * @param evictionCandidates   SegmentMetadata eviction candidates, obtained by calling getEvictionCandidates.
     * @param sequenceNumberCutoff A Sequence Number that indicates the cutoff threshold. A Segment is eligible for eviction
     *                             if it has a LastUsed value smaller than this threshold.
     * @return A Collection of SegmentMetadata for those segments that were actually removed. This will always be a
     * subset of cleanupCandidates.
     */
    Collection<SegmentMetadata> cleanup(Collection<SegmentMetadata> evictionCandidates, long sequenceNumberCutoff);

    /**
     * Evicts Extended Attributes from those Segments which have exceeded the maximum Extended Attribute count.
     *
     * @param maximumAttributeCount The maximum number of Extended Attributes per Segment. Cleanup will only be performed
     *                              for a Segment if that Segment's Metadata has at least this many Extended Attributes
     *                              in memory.
     * @param sequenceNumberCutoff  The Operation Sequence number before which it is safe to remove an Extended Attribute.
     *                              Any Extended Attribute that has been used after this cutoff will not be removed.
     * @return The number of Extended Attributes removed.
     */
    int cleanupExtendedAttributes(int maximumAttributeCount, long sequenceNumberCutoff);
}
