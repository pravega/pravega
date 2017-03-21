/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server;

import java.util.Collection;

/**
 * Defines an updateable StreamSegment Metadata.
 */
public interface UpdateableContainerMetadata extends ContainerMetadata, RecoverableMetadata, TruncationMarkerRepository {
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

    /**
     * Maps a new StreamSegment Name to the given Id.
     *
     * @param streamSegmentName The case-sensitive name of the StreamSegment to map.
     * @param streamSegmentId   The Id of the StreamSegment.
     * @return An UpdateableSegmentMetadata that represents the metadata for the newly mapped StreamSegment.
     */
    UpdateableSegmentMetadata mapStreamSegmentId(String streamSegmentName, long streamSegmentId);

    /**
     * Maps a new StreamSegment to its Parent StreamSegment.
     * This is used for Transactions that are dependent on their parent StreamSegments.
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
     */
    Collection<Long> getAllStreamSegmentIds();

    /**
     * Marks the StreamSegment and all child StreamSegments as deleted.
     *
     * @param streamSegmentName The name of the StreamSegment to delete.
     * @return A Collection of SegmentMetadatas for the Segments that have been deleted. This includes the given StreamSegment,
     * as well as any child StreamSegments that have been deleted.
     */
    Collection<SegmentMetadata> deleteStreamSegment(String streamSegmentName);

    /**
     * Gets the next available Operation Sequence Number. Atomically increments the value by 1 with every call.
     *
     * @return The next available Operation Sequence Number.
     * @throws IllegalStateException If the Metadata is in Recovery Mode.
     */
    long nextOperationSequenceNumber();

    /**
     * Gets the StreamSegmentMetadata mapped to the given StreamSegment Id.
     *
     * @param streamSegmentId The Id of the StreamSegment to query for.
     * @return The mapped StreamSegmentMetadata, or null if none is.
     */
    @Override
    UpdateableSegmentMetadata getStreamSegmentMetadata(long streamSegmentId);

    /**
     * Gets a collection of SegmentMetadata referring to Segments that are currently eligible for removal.
     *
     * @param sequenceNumberCutoff A Sequence Number that indicates the cutoff threshold. A Segment is eligible for eviction
     *                             if it has a LastUsed value smaller than this threshold.
     * @return The collection of SegmentMetadata that can be cleaned up.
     */
    Collection<SegmentMetadata> getEvictionCandidates(long sequenceNumberCutoff);

    /**
     * Evicts the StreamSegments that match the given SegmentMetadata, but only if they are still eligible for removal.
     *
     * @param evictionCandidates SegmentMetadata eviction candidates, obtained by calling getEvictionCandidates.
     * @param sequenceNumberCutoff A Sequence Number that indicates the cutoff threshold. A Segment is eligible for eviction
     *                             if it has a LastUsed value smaller than this threshold.
     * @return A Collection of SegmentMetadata for those segments that were actually removed. This will always be a
     * subset of cleanupCandidates.
     */
    Collection<SegmentMetadata> cleanup(Collection<SegmentMetadata> evictionCandidates, long sequenceNumberCutoff);
}
