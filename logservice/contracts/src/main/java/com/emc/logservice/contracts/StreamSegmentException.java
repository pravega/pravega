package com.emc.logservice.contracts;

/**
 * Represents a StreamSegment-specific exception.
 */
public abstract class StreamSegmentException extends StreamingException {
    private final String streamSegmentName;

    /**
     * Creates a new instance of the StreamSegmentException class.
     * @param streamSegmentName The name of the StreamSegment.
     * @param message The message for this exception.
     */
    public StreamSegmentException(String streamSegmentName, String message) {
        this(streamSegmentName, message, null);
    }

    /**
     * Creates a new instance of the StreamSegmentException class.
     * @param streamSegmentName The name of the StreamSegment.
     * @param message The message for this exception.
     * @param cause The causing exception.
     */
    public StreamSegmentException(String streamSegmentName, String message, Throwable cause) {
        super(String.format("%s (%s).", message, streamSegmentName), cause);
        this.streamSegmentName = streamSegmentName;
    }

    /**
     * Gets a value indicating the StreamSegment Name.
     * @return
     */
    public String getStreamSegmentName() {
        return this.streamSegmentName;
    }
}
