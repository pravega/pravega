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
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import java.util.HashMap;

/**
 * Defines an immutable StreamSegment Metadata.
 */
public interface SegmentMetadata extends SegmentProperties {
    /**
     * The maximum number of attributes that a single Segment can have at any given time. Due to serialization constraints
     * there needs to be a hard limit as to how many attributes each segment can have.
     */
    int MAXIMUM_ATTRIBUTE_COUNT = 1024;

    /**
     * Defines an attribute value that denotes a missing value.
     */
    long NULL_ATTRIBUTE_VALUE = Long.MIN_VALUE; //This is the same as WireCommands.NULL_ATTRIBUTE_VALUE

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
    default SegmentProperties getSnapshot() {
        return StreamSegmentInformation.from(this).attributes(new HashMap<>(getAttributes())).build();
    }

    /**
     * Determines whether the Segment represented by this SegmentMetadata is a Transaction.
     *
     * @return True if Transaction, False otherwise.
     */
    default boolean isTransaction() {
        return getParentId() != ContainerMetadata.NO_STREAM_SEGMENT_ID;
    }
}
