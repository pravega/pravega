/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
public class StreamSegmentTruncatedException extends StreamingException {
    /**
     * Creates a new instance of the StreamSegmentTruncatedException class.
     *
     * @param startOffset First valid offset of the StreamSegment.
     */
    public StreamSegmentTruncatedException(long startOffset) {
        this(startOffset, "Segment truncated");
    }

    /**
     * Creates a new instance of the StreamSegmentTruncatedException class.
     *
     * @param startOffset First valid offset of the StreamSegment.
     * @param message     Custom error message.
     */
    public StreamSegmentTruncatedException(long startOffset, String message) {
        super(getMessage(startOffset, message));
    }

    private static String getMessage(long startOffset, String message) {
        return String.format("%s: Lowest accessible offset is %d.", message, startOffset);
    }
}
