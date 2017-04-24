/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.retryable;

import io.pravega.common.ExceptionHelpers;

/**
 * Retryable exception. Throw this when you want to let the caller know that this exception is transient and
 * warrants another retry.
 * This is used to wrap exceptions from retrieables list into a new RetryableException
 */
public interface RetryableException {

    static boolean isRetryable(Throwable e) {
        Throwable cause = ExceptionHelpers.getRealException(e);

        return cause instanceof RetryableException;
    }
}
