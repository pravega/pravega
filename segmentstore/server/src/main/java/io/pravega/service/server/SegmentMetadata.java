/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.service.server;

import io.pravega.service.contracts.SegmentProperties;
import io.pravega.service.contracts.StreamSegmentInformation;

import java.util.HashMap;

/**
 * Defines an immutable StreamSegment Metadata.
 */
public interface SegmentMetadata extends SegmentProperties {
    /**
     * Defines an attribute value that denotes a missing value.
     */
    Long NULL_ATTRIBUTE_VALUE = Long.MIN_VALUE;

    /**
     * Gets a value indicating the id of this StreamSegment.
     */
    long getId();

    /**
     * Gets a value indicating the id of this StreamSegment's parent.
     */
    long getParentId();

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
     * Gets a value indicating the length of this entire StreamSegment (the part in Storage + the part in DurableLog).
     */
    long getDurableLogLength();

    /**
     * Gets a value representing the when the Segment was last used. The meaning of this value is implementation specific,
     * however higher values should indicate it was used more recently.
     */
    long getLastUsed();

    /**
     * Creates a new SegmentProperties instance with current information from this SegmentMetadata object.
     *
     * @return The new SegmentProperties instance. This object is completely detached from the SegmentMetadata from which
     * it was created (changes to the base object will not be reflected in the result).
     */
    default SegmentProperties getSnapshot() {
        return new StreamSegmentInformation(this, new HashMap<>(getAttributes()));
    }
}
