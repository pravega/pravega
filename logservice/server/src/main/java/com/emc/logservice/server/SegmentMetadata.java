package com.emc.logservice.server;

import com.emc.logservice.contracts.SegmentProperties;

/**
 * Defines an immutable StreamSegment Metadata.
 */
public interface SegmentMetadata extends SegmentProperties {
    /**
     * Gets a value indicating the id of this StreamSegment.
     *
     * @return
     */
    long getId();

    /**
     * Gets a value indicating the id of this StreamSegment's parent.
     *
     * @return
     */
    long getParentId();

    /**
     * Gets a value indicating the length of this StreamSegment for the part that exists in Storage Only.
     *
     * @return
     */
    long getStorageLength();

    /**
     * Gets a value indicating the length of this entire StreamSegment (the part in Storage + the part in DurableLog).
     *
     * @return
     */
    long getDurableLogLength();
}
