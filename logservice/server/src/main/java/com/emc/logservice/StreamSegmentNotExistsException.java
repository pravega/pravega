package com.emc.logservice;

/**
 * Represents an exception that is thrown when a StreamSegment that does not exist is attempted to be accessed.
 */
public class StreamSegmentNotExistsException extends StreamSegmentException {
    public StreamSegmentNotExistsException(String streamName) {
        super(streamName, "The StreamSegment does not exist.");
    }
}
