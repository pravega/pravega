/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.contracts;

/**
 * Exception that is thrown whenever an Append Operation failed because of inconsistent AppendContext.EventNumbers.
 */
public class BadEventNumberException extends StreamSegmentException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the BadEventNumberException class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param lastEventNumber   The last known Event Number for the StreamSegment.
     * @param actualEventNumber The Event Number that was given as part of the Operation.
     */
    public BadEventNumberException(String streamSegmentName, long lastEventNumber, long actualEventNumber) {
        super(streamSegmentName, getMessage(lastEventNumber, actualEventNumber));
    }

    private static String getMessage(long expectedOffset, long actualOffset) {
        return String.format("Bad EventNumber. Expected greater than %d, given %d.", expectedOffset, actualOffset);
    }
}
