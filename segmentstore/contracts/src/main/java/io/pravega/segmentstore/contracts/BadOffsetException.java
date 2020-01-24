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

import lombok.Getter;

/**
 * Exception that is thrown whenever a Write failed due to a bad offset.
 */
public class BadOffsetException extends StreamSegmentException {
    private static final long serialVersionUID = 1L;
    @Getter
    private final long expectedOffset;
    @Getter
    private final long givenOffset;

    /**
     * Creates a new instance of the BadOffsetException class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param expectedOffset    The expected offset for the Operation.
     * @param givenOffset       The offset that was given as part of the operation.
     */
    public BadOffsetException(String streamSegmentName, long expectedOffset, long givenOffset) {
        super(streamSegmentName, getMessage(expectedOffset, givenOffset));
        this.expectedOffset = expectedOffset;
        this.givenOffset = givenOffset;
    }

    /**
     * Creates a new instance of the BadOffsetException class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param expectedOffset The offset that was expected.
     * @param givenOffset The offset that was actually supplied.
     * @param message           The message of the exception.
     */
    public BadOffsetException(String streamSegmentName, long expectedOffset, long givenOffset, String message) {
        super(streamSegmentName, message);
        this.expectedOffset = expectedOffset;
        this.givenOffset = givenOffset;
    }

    private static String getMessage(long expectedOffset, long givenOffset) {
        return String.format("Bad Offset. Expected %d, given %d.", expectedOffset, givenOffset);
    }
}
