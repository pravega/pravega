/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.contracts;

/**
 * Represents an exception that is thrown when an operation that is incompatible with a sealed StreamSegment is attempted on
 * a sealed StreamSegment.
 */
public class StreamSegmentSealedException extends StreamSegmentException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the StreamSegmentSealedException class.
     *
     * @param streamSegmentName The name of the StreamSegment that is sealed.
     */
    public StreamSegmentSealedException(String streamSegmentName) {
        super(streamSegmentName, null);
    }

    /**
     * Creates a new instance of the StreamSegmentSealedException class.
     *
     * @param streamSegmentName The name of the StreamSegment that is sealed.
     * @param cause             Actual cause of the exception.
     */
    public StreamSegmentSealedException(String streamSegmentName, Throwable cause) {
        super(streamSegmentName, "The StreamSegment is closed for appends.", cause);
    }
}
