package com.emc.logservice.contracts;

/**
 * Represents an exception that is thrown when an operation that is incompatible with a sealed StreamSegment is attempted on
 * a sealed StreamSegment.
 */
public class StreamSegmentSealedException extends StreamSegmentException {
    public StreamSegmentSealedException(String streamName) {
        super(streamName, "The StreamSegment is closed for appends.");
    }
}
