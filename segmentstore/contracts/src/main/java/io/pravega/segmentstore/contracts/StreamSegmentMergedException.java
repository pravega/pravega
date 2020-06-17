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
 * Represents an exception that is thrown when a StreamSegment that has already been merged is attempted to be accessed.
 */
public class StreamSegmentMergedException extends StreamSegmentException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public StreamSegmentMergedException(String streamName) {
        super(streamName, "The StreamSegment has been merged.");
    }
}