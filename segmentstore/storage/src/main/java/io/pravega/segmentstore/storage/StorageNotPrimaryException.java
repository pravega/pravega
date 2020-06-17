/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import io.pravega.segmentstore.contracts.StreamSegmentException;

/**
 * Indicates that a particular Storage Instance is no longer the Primary Writer for a Segment.
 */
public class StorageNotPrimaryException extends StreamSegmentException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the StorageNotPrimaryException class.
     *
     * @param streamSegmentName The name of the segment for which the Storage is no longer primary.
     */
    public StorageNotPrimaryException(String streamSegmentName) {
        this(streamSegmentName, null, null);
    }

    /**
     * Creates a new instance of the StorageNotPrimaryException class.
     *
     * @param streamSegmentName The name of the segment for which the Storage is no longer primary.
     * @param cause             The causing exception.
     */
    public StorageNotPrimaryException(String streamSegmentName, Throwable cause) {
        this(streamSegmentName, null, cause);
    }

    public StorageNotPrimaryException(String streamSegmentName, String message) {
        this(streamSegmentName, message, null);
    }

    public StorageNotPrimaryException(String streamSegmentName, String message, Throwable cause) {
        super(streamSegmentName, "The current instance is no longer the primary writer for this StreamSegment." + (message == null ? "" : " " + message),
                cause);
    }
}
