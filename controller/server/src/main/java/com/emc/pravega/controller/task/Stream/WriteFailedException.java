/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.task.Stream;

import com.emc.pravega.controller.retryable.RetryableException;

/**
 * Exception thrown when write to a pravega stream fails.
 */
public class WriteFailedException extends RuntimeException implements RetryableException {

    public WriteFailedException() {
        super();
    }

    public WriteFailedException(String message) {
        super(message);
    }

    public WriteFailedException(Throwable cause) {
        super(cause);
    }

    public WriteFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
