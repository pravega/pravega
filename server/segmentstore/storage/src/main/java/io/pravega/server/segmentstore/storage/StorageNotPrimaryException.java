/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.server.segmentstore.storage;

import io.pravega.server.segmentstore.contracts.StreamSegmentException;

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
        this(streamSegmentName, null);
    }

    public StorageNotPrimaryException(String streamSegmentName, String message) {
        super(streamSegmentName, "The current instance is no longer the primary writer for this StreamSegment." + (message == null ? "" : " ") + message);
    }
}
