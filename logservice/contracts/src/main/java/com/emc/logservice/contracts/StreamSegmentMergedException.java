package com.emc.logservice.contracts;

/**
 * Represents an exception that is thrown when a StreamSegment that has already been merged is attempted to be accessed.
 */
public class StreamSegmentMergedException extends StreamSegmentException {
    public StreamSegmentMergedException(String streamName) {
        super(streamName, "The StreamSegment has been merged.");
    }
}