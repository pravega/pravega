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

public class StreamSegmentNotSealedException extends StreamSegmentException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the StreamSegmentNotSealedException class.
     *
     * @param streamSegmentName The name of the StreamSegment that is not sealed.
     */
    public StreamSegmentNotSealedException(String streamSegmentName) {
        super(streamSegmentName, "The StreamSegment is not sealed.", null);
    }
}