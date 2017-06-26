/**
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
 * Exception thrown attempting an operation that is not allowed to be performed at the moment.
 */
public class OperationNotAllowed extends RuntimeException implements RetryableException {

    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "State transition for path %s.";

    /**
     * Creates a new instance of OperationNotAllowed class.
     *
     * @param value value
     */
    public OperationNotAllowed(final String value) {
        super(String.format(FORMAT_STRING, value));
    }
}
