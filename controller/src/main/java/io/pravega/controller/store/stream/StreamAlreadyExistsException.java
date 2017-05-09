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

/**
 * Exception thrown when an attempt is made to create stream with duplicate name.
 */
public class StreamAlreadyExistsException extends RuntimeException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Stream %s already exists.";

    /**
     * Creates a new instance of StreamAlreadyExistsException class.
     *
     * @param name duplicate stream name
     */
    public StreamAlreadyExistsException(final String name) {
        super(String.format(FORMAT_STRING, name));
    }

    /**
     * Creates a new instance of StreamAlreadyExistsException class.
     *
     * @param name  duplicate stream name
     * @param cause error cause
     */
    public StreamAlreadyExistsException(final String name, final Throwable cause) {
        super(String.format(FORMAT_STRING, name), cause);
    }
}
