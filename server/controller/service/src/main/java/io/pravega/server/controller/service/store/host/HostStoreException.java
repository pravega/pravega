/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.server.controller.service.store.host;

import io.pravega.server.controller.service.retryable.RetryableException;

/**
 * This exception is thrown on errors from the HostControllerStore implementation.
 */
public class HostStoreException extends RuntimeException implements RetryableException {

    /**
     * Create a HostStoreException using a text cause.
     *
     * @param message   The cause of the exception.
     */
    public HostStoreException(String message) {
        super(message);
    }

    /**
     * Create a HostStoreException using a text cause.
     *
     * @param message   The cause of the exception.
     * @param cause     Any existing exception that needs to be wrapped.
     */
    public HostStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
