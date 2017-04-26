/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.test.integration.service.selftest;

import lombok.Getter;

/**
 * Exception thrown whenever a validation error occurred.
 */
class ValidationException extends Exception {
    @Getter
    private final String segmentName;

    /**
     * Creates a new instance of the ValidationException class.
     *
     * @param segmentName      The name of the Segment that failed validation.
     * @param validationResult The ValidationResult that triggered this.
     */
    ValidationException(String segmentName, ValidationResult validationResult) {
        super(String.format("Segment = %s, Offset = %s, Reason = %s", segmentName, validationResult.getSegmentOffset(), validationResult.getFailureMessage()));
        this.segmentName = segmentName;
    }
}
