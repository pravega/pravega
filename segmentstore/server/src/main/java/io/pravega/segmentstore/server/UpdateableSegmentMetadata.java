/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.common.util.ImmutableDate;
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
     * Sets the first offset available for reading for this StreamSegment. This essentially marks the Segment as truncated
     * at this offset.
     *
     * @param value The new first available offset.
     * @throws IllegalArgumentException If the value is invalid.
     */
    void setStartOffset(long value);

    /**
     * Sets the current Length for this StreamSegment.
     *
     * @param value The new length.
     * @throws IllegalArgumentException If the value is invalid.
     */
    void setLength(long value);

    /**
     * Marks this StreamSegment as sealed for modifications.
     */
    void markSealed();

    /**
     * Marks this StreamSegment as sealed in Storage.
     * This is different from {@link #markSealed()} in that {@link #markSealed()} indicates it was sealed in the Metadata,
     * while this indicates this fact has been persisted in Storage.
     */
    void markSealedInStorage();

    /**
     * Marks this StreamSegment as deleted.
     */
    void markDeleted();

    /**
     * Marks this StreamSegment as deleted in Storage.
     * This is different from markDeleted() in that markDeleted() indicates it was deleted in the Metadata, while this
     * indicates the Segment has been deleted from Storage.
     */
    void markDeletedInStorage();

    /**
     * Marks this StreamSegment as merged.
     */
    void markMerged();

    /**
     * Marks this StreamSegment as pinned to memory. See {@link SegmentMetadata#isPinned()} for more details.
     */
    void markPinned();

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
     * Updates this instance of the {@link UpdateableSegmentMetadata} to have the same information as the other one.
     *
     * @param other The SegmentMetadata to copy from.
     * @throws IllegalArgumentException If the other {@link SegmentMetadata} refers to a different StreamSegment.
     */
    void copyFrom(SegmentMetadata other);

    /**
     * Sets a value indicating when the Segment was last used.
     *
     * @param value The value to set. The meaning of this value is implementation-specific, however higher values should
     *              indicate it was used more recently.
     */
    void setLastUsed(long value);

    /**
     * Refreshes the {@link #getType()} property based on the current {@link #getAttributes()} values.
     */
    void refreshType();
}