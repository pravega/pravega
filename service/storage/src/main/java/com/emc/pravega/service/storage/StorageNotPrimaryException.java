/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage;

import com.emc.pravega.service.contracts.StreamSegmentException;

/**
 * Indicates that a particular Storage Instance is no longer the Primary Writer for a Segment.
 */
public class StorageNotPrimaryException extends StreamSegmentException {
    /**
     * Creates a new instance of the StorageNotPrimaryException class.
     *
     * @param streamSegmentName The name of the segment for which the Storage is no longer primary.
     */
    public StorageNotPrimaryException(String streamSegmentName) {
        super(streamSegmentName, "The current instance is no longer the primary writer for this StreamSegment.");
    }
}
