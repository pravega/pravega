/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.task.Stream;

import io.pravega.controller.retryable.RetryableException;

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
