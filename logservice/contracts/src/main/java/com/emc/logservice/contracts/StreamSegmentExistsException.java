package com.emc.logservice.contracts;

/**
 * Represents an exception that is thrown when a StreamSegment that already exists is attempted to be created again.
 */
public class StreamSegmentExistsException extends StreamSegmentException {
    public StreamSegmentExistsException(String streamName) {
        super(streamName, "The StreamSegment exists already.");
    }
}