/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.contracts;

/**
 * Represents an exception that is thrown when a StreamSegment that does not exist is attempted to be accessed.
 */
public class StreamSegmentNotExistsException extends StreamSegmentException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    public StreamSegmentNotExistsException(String streamSegmentName) {
        super(streamSegmentName, "The StreamSegment does not exist.");
    }

    public StreamSegmentNotExistsException(String streamSegmentName, Throwable cause) {
        super(streamSegmentName, "The StreamSegment does not exist.", cause);
    }
}
