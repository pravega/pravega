/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.contracts;

/**
 * An Exception that is related to a particular Container.
 */
public abstract class StreamSegmentException extends StreamingException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private final String streamSegmentName;

    /**
     * Creates a new instance of the StreamSegmentException class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param message           The message for this exception.
     */
    public StreamSegmentException(String streamSegmentName, String message) {
        this(streamSegmentName, message, null);
    }

    /**
     * Creates a new instance of the StreamSegmentException class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param message           The message for this exception.
     * @param cause             The causing exception.
     */
    public StreamSegmentException(String streamSegmentName, String message, Throwable cause) {
        super(String.format("[Segment '%s'] %s", streamSegmentName, message), cause);
        this.streamSegmentName = streamSegmentName;
    }

    /**
     * Gets a value indicating the StreamSegment Name.
     */
    public String getStreamSegmentName() {
        return this.streamSegmentName;
    }
}
