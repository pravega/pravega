/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.logs.operations;

import java.util.Map;
import java.util.UUID;

/**
 * Defines a mapping between a StreamSegment Name and its Id.
 */
public interface StreamSegmentMapping {
    /**
     * Gets a value indicating the Id of the StreamSegment.
     */
    long getStreamSegmentId();

    /**
     * Gets a value indicating the Name of the StreamSegment.
     */
    String getStreamSegmentName();

    /**
     * Gets a value indicating the Length of the StreamSegment at the time of the mapping.
     */
    long getLength();

    /**
     * Gets a value indicating whether the StreamSegment is currently sealed at the time of the mapping.
     */
    boolean isSealed();

    /**
     * Gets the Attributes for the StreamSegment at the time of the mapping.
     */
    Map<UUID, Long> getAttributes();
}
