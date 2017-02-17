/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
