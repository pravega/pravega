/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.test.integration.service.selftest;

import com.emc.pravega.common.Exceptions;
import lombok.Getter;
import lombok.Setter;

import java.time.Duration;

import static com.emc.pravega.test.integration.service.selftest.AppendContentGenerator.HEADER_LENGTH;

/**
 * Represents the result of a validation process.
 */
class ValidationResult {
    //region Members
    @Getter
    @Setter
    private String source;
    @Getter
    @Setter
    private Duration elapsed;
    @Getter
    @Setter
    private long segmentOffset;

    /**
     * Indicates whether more data is needed in order to make a proper determination.
     * If this is true, it does not mean that the test failed.
     */
    @Getter
    private boolean moreDataNeeded;

    /**
     * Indicates the length of the validated append. This value is undefined if isSuccess() == false.
     */
    @Getter
    private int length;

    /**
     * Indicates the failure message. This is undefined if isFailed() == false.
     */
    @Getter
    private String failureMessage;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ValidationResult class. Not to be used externally (use the static factory methods instead).
     */
    private ValidationResult() {
        this.moreDataNeeded = false;
        this.length = HEADER_LENGTH;
        this.failureMessage = null;
        this.elapsed = null;
    }

    /**
     * Creates a new ValidationResult for a failed verification.
     */
    static ValidationResult failed(String message) {
        Exceptions.checkNotNullOrEmpty(message, "message");
        ValidationResult result = new ValidationResult();
        result.failureMessage = message;
        return result;
    }

    /**
     * Creates a new ValidationResult for an inconclusive verification, when more data is needed to determine correctness.
     */
    static ValidationResult moreDataNeeded() {
        ValidationResult result = new ValidationResult();
        result.moreDataNeeded = true;
        return result;
    }

    /**
     * Creates a new ValidationResult for a successful test.
     */
    static ValidationResult success(int length) {
        ValidationResult result = new ValidationResult();
        result.length = HEADER_LENGTH + length;
        return result;
    }

    //endregion

    //region Properties

    /**
     * Gets a value indicating whether the verification failed.
     */
    boolean isFailed() {
        return this.failureMessage != null;
    }

    /**
     * Gets a value indicating whether the verification succeeded.
     */
    boolean isSuccess() {
        return !isFailed() && !isMoreDataNeeded();
    }

    @Override
    public String toString() {
        if (isFailed()) {
            return String.format("Failed (Source=%s, Offset=%d, Reason=%s)", this.source, this.segmentOffset, this.failureMessage);
        } else if (isMoreDataNeeded()) {
            return String.format("More data needed (Source=%s, Offset=%d)", this.source, this.segmentOffset);
        } else {
            return String.format("Success (Source=%s, Offset=%d, Length = %d)", this.source, this.segmentOffset, this.length);
        }
    }

    //endregion
}
