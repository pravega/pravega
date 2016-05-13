package com.emc.logservice;

/**
 * Defines a Source for StreamSegmentMetadata.
 */
public interface StreamSegmentMetadataSource
{
    /**
     * Gets the StreamSegmentMetadata mapped to the given StreamSegment Id.
     * @param streamSegmentId The Id of the StreamSegment to query for.
     * @return The mapped StreamSegmentMetadata, or null if none is.
     */
    ReadOnlyStreamSegmentMetadata getStreamSegmentMetadata(long streamSegmentId);
}
