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
 * Exception thrown when data identified by given identifier is not found in the metadata.
 */
public class DataNotFoundException extends RuntimeException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Data %s not found.";

    /**
     * Creates a new instance of DataNotFoundException class.
     *
     * @param name missing data identifier
     */
    public DataNotFoundException(final String name) {
        super(String.format(FORMAT_STRING, name));
    }

    /**
     * Creates a new instance of DataNotFoundException class.
     *
     * @param name  missing data identifier
     * @param cause error cause
     */
    public DataNotFoundException(final String name, final Throwable cause) {
        super(String.format(FORMAT_STRING, name), cause);
    }
}
