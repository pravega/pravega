/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller;

import com.emc.pravega.common.ExceptionHelpers;

/**
 * Retryable exception. Throw this when you want to let the caller know that this exception is transient and
 * warrants another retry.
 */
public abstract class RetryableException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of RetryableException class.
     *
     * @param reason reason for failure
     */
    public RetryableException(final String reason) {
        super(reason);
    }

    /**
     * Creates a new instance of RetryableException class.
     *
     * @param cause reason for failure
     */
    public RetryableException(final Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new instance of RetryableException class.
     *
     * @param reason reason for failure
     * @param cause  error cause
     */
    public RetryableException(final String reason, final Throwable cause) {
        super(reason, cause);
    }

    /**
     * Check if the exception is a subclass of retryable.
     *
     * @param e exception thrown
     * @return whether it can be assigned to Retryable
     */
    public static boolean isRetryable(Throwable e) {
        Throwable cause = ExceptionHelpers.getRealException(e);

        return cause instanceof RetryableException;
    }

    public static void throwRetryableOrElseRuntime(Throwable e) throws RetryableException {
        if (e != null) {
            Throwable cause = ExceptionHelpers.getRealException(e);
            if (cause != null && RetryableException.isRetryable(cause)) {
                throw (RetryableException) cause;
            } else {
                throw new RuntimeException(cause);
            }
        }
    }

}
