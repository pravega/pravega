/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.server.segmentstore.contracts;

/**
 * Exception that is thrown whenever an Append Operation failed because of bad Attribute Updates.
 */
public class BadAttributeUpdateException extends StreamSegmentException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the BadAttributeUpdateException class.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param attributeUpdate   The AttributeUpdate that failed the check.
     * @param errorMessage      The Event Number that was given as part of the Operation.
     */
    public BadAttributeUpdateException(String streamSegmentName, AttributeUpdate attributeUpdate, String errorMessage) {
        super(streamSegmentName, getMessage(attributeUpdate, errorMessage));
    }

    private static String getMessage(AttributeUpdate attributeUpdate, String errorMessage) {
        return String.format("Bad Attribute Update (%s): %s.", attributeUpdate, errorMessage);
    }
}
