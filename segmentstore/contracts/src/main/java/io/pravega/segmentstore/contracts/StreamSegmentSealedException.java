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
 * Represents an exception that is thrown when an operation that is incompatible with a sealed StreamSegment is attempted on
 * a sealed StreamSegment.
 */
public class StreamSegmentSealedException extends StreamSegmentException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the StreamSegmentSealedException class.
     *
     * @param streamSegmentName The name of the StreamSegment that is sealed.
     */
    public StreamSegmentSealedException(String streamSegmentName) {
        super(streamSegmentName, "The StreamSegment is sealed and cannot be modified.", null);
    }

    /**
     * Creates a new instance of the StreamSegmentSealedException class.
     *
     * @param streamSegmentName The name of the StreamSegment that is sealed.
     * @param cause             Actual cause of the exception.
     */
    public StreamSegmentSealedException(String streamSegmentName, Throwable cause) {
        super(streamSegmentName, "The StreamSegment is sealed and cannot be modified.", cause);
    }
}
