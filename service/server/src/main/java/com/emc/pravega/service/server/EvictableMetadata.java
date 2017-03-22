/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.server;

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
}
