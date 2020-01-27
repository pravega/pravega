/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.contracts;

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
