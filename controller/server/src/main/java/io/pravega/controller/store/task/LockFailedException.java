/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.store.task;

import io.pravega.controller.retryable.RetryableException;

/**
 * Lock failed exception.
 */
public class LockFailedException extends RuntimeException implements RetryableException {
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Failed locking resource %s.";

    /**
     * Creates a new instance of LockFailedException class.
     *
     * @param name resource on which lock failed
     */
    public LockFailedException(final String name) {
        super(String.format(FORMAT_STRING, name));
    }

    /**
     * Creates a new instance of LockFailedException class.
     *
     * @param name  resource on which lock failed
     * @param cause error cause
     */
    public LockFailedException(final String name, final Throwable cause) {
        super(String.format(FORMAT_STRING, name), cause);
    }
}
