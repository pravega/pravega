/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.contracts;

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