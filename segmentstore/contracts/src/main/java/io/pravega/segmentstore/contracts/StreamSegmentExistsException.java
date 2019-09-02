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
 * Represents an exception that is thrown when a StreamSegment that already exists is attempted to be created again.
 */
public class StreamSegmentExistsException extends StreamSegmentException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the StreamSegmentExistsException.
     *
     * @param streamSegmentName The name of the Segment.
     */
    public StreamSegmentExistsException(String streamSegmentName) {
        super(streamSegmentName, "The StreamSegment exists already.");
    }

    /**
     * Creates a new instance of the StreamSegmentExistsException.
     *
     * @param streamSegmentName The name of the Segment.
     * @param cause             Actual cause of the exception.
     */
    public StreamSegmentExistsException(String streamSegmentName, Throwable cause) {
        super(streamSegmentName, "The StreamSegment exists already", cause);
    }
}