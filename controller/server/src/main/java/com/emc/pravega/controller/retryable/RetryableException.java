/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.retryable;

/**
 * Retryable exception. Throw this when you want to let the caller know that this exception is transient and
 * warrants another retry.
 * This is used to wrap exceptions from retrieables list into a new RetryableException
 */
public final class RetryableException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of RetryableException class.
     *
     * @param cause reason for failure
     */
    RetryableException(final Throwable cause) {
        super(cause);
    }
}
