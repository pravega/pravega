package com.emc.logservice;

/**
 * Defines a Collection for StreamSegmentMetadata.
 */
public interface SegmentMetadataCollection {

    /**
     * Reserved value that indicates a missing StreamSegmentId. No valid StreamSegment can have this ID.
     */
    long NoStreamSegmentId = Long.MIN_VALUE;

    /**
     * Gets the Id of the StreamSegment with given name.
     *
     * @param streamSegmentName The case-sensitive StreamSegment name.
     * @return The Id of the StreamSegment, or NoStreamSegmentId if the Metadata has no knowledge of it.
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
