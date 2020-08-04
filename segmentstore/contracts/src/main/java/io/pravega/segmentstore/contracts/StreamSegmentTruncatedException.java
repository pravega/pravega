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
 * An Exception that indicates a StreamSegment has been truncated and certain offsets cannot be accessed anymore.
 */
public class StreamSegmentTruncatedException extends StreamSegmentException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the StreamSegmentTruncatedException class.
     *
     * @param segmentName Name of the truncated Segment.
     * @param message     Message.
     * @param ex          Causing exception.
     */
    public StreamSegmentTruncatedException(String segmentName, String message, Throwable ex) {
        super(segmentName, message, ex);
    }

    /**
     * Creates a new instance of the StreamSegmentTruncatedException class.
     *
     * @param startOffset First valid offset of the StreamSegment.
     */
    public StreamSegmentTruncatedException(long startOffset) {
        super("", String.format("Segment truncated: Lowest accessible offset is %d.", startOffset));
    }
}
