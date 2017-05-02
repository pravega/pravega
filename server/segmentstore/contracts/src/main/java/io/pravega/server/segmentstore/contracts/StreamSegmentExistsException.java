/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.contracts;

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