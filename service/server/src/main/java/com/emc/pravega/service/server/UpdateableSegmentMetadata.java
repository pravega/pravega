/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server;

import com.emc.pravega.common.util.ImmutableDate;
import java.util.Map;
import java.util.UUID;

/**
 * Defines an updateable StreamSegment Metadata.
 */
public interface UpdateableSegmentMetadata extends SegmentMetadata {
    /**
     * Sets the current StorageLength for this StreamSegment.
     *
     * @param value The StorageLength to set.
     * @throws IllegalArgumentException If the value is invalid.
     */
    void setStorageLength(long value);

    /**
     * Sets the current DurableLog Length for this StreamSegment.
     *
     * @param value The new DurableLog length.
     * @throws IllegalArgumentException If the value is invalid.
     */
    void setDurableLogLength(long value);

    /**
     * Marks this StreamSegment as sealed for modifications.
     */
    void markSealed();

    /**
     * Marks this StreamSegment as sealed in Storage.
     * This is different from markSealed() in that markSealed() indicates it was sealed in DurableLog, which this indicates
     * this fact has been persisted in Storage.
     */
    void markSealedInStorage();

    /**
     * Marks this StreamSegment as deleted.
     */
    void markDeleted();

    /**
     * Marks this StreamSegment as merged.
     */
    void markMerged();

    /**
     * Sets/Updates the attributes for this StreamSegment to the exact values provided.
     *
     * @param attributeValues The values to set/update.
     */
    void updateAttributes(Map<UUID, Long> attributeValues);

    /**
     * Sets the Last Modified date.
     *
     * @param date The Date to set.
     */
    void setLastModified(ImmutableDate date);

    /**
     * Updates this instance of the UpdateableSegmentMetadata to have the same information as the other one.
     *
     * @param other The SegmentMetadata to copy from.
     * @throws IllegalArgumentException If the other SegmentMetadata refers to a different StreamSegment.
     */
    void copyFrom(SegmentMetadata other);

    /**
     * Sets a value indicating when the Segment was last used.
     *
     * @param value The value to set. The meaning of this value is implementation-specific, however higher values should
     *              indicate it was used more recently.
     */
    void setLastUsed(long value);
}