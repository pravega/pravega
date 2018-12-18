/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.contracts.SegmentProperties;

/**
 * Defines an immutable StreamSegment Metadata.
 */
public interface SegmentMetadata extends SegmentProperties {
    /**
     * Gets a value indicating the id of this StreamSegment.
     */
    long getId();

    /**
     * Gets a value indicating the id of the Container this StreamSegment belongs to.
     */
    int getContainerId();

    /**
     * Gets a value indicating whether this StreamSegment has been merged into another.
     */
    boolean isMerged();

    /**
     * Gets a value indicating whether this StreamSegment has been sealed in Storage.
     * This is different from isSealed(), which returns true if the StreamSegment has been sealed in DurableLog or in Storage.
     */
    boolean isSealedInStorage();

    /**
     * Gets a value indicating the length of this StreamSegment for the part that exists in Storage Only.
     */
    long getStorageLength();

    /**
     * Gets a value representing the when the Segment was last used. The meaning of this value is implementation specific,
     * however higher values should indicate it was used more recently.
     */
    long getLastUsed();

    /**
     * Gets a value indicating whether this instance of the SegmentMetadata is still active. As long as the Segment is
     * registered in the ContainerMetadata, this will return true. If this returns false, it means this SegmentMetadata
     * instance has been evicted from the ContainerMetadata (for whatever reasons) and is should be considered stale.
     *
     * @return Whether this instance of the SegmentMetadata is active or not.
     */
    boolean isActive();

    /**
     * Creates a new SegmentProperties instance with current information from this SegmentMetadata object.
     *
     * @return The new SegmentProperties instance. This object is completely detached from the SegmentMetadata from which
     * it was created (changes to the base object will not be reflected in the result).
     */
    SegmentProperties getSnapshot();

    /**
     * Gets a value indicating whether this {@link SegmentMetadata} instance is pinned to memory. If pinned, this metadata
     * will never be evicted by the owning metadata (even if there is eviction pressure and this Segment meets all other
     * eviction criteria).
     *
     * Notes:
     * - This will still be cleared out of the metadata if {@link UpdateableContainerMetadata#reset()} is invoked.
     * - This has no bearing on the eviction of the Segment's Extended Attributes.
     *
     * @return True if pinned, false otherwise.
     */
    boolean isPinned();
}
