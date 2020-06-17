/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.host;

import io.pravega.controller.retryable.RetryableException;

/**
 * This exception is thrown on errors from the HostControllerStore implementation.
 */
public class HostStoreException extends RuntimeException implements RetryableException {

    private static final long serialVersionUID = 1L;

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
