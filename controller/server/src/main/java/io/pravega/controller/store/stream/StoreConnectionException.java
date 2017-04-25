/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.store.stream;

import io.pravega.controller.retryable.RetryableException;

/**
 * StoreConnectionException exception.
 */
public class StoreConnectionException extends RuntimeException implements RetryableException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of StoreConnectionException class.
     *
     * @param reason reason for failure
     */
    public StoreConnectionException(final String reason) {
        super(reason);
    }

    /**
     * Creates a new instance of StoreConnectionException class.
     *
     * @param cause reason for failure
     */
    public StoreConnectionException(final Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new instance of StoreConnectionException class.
     *
     * @param reason reason for failure
     * @param cause  error cause
     */
    public StoreConnectionException(final String reason, final Throwable cause) {
        super(reason, cause);
    }
}
