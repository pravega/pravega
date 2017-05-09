/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
