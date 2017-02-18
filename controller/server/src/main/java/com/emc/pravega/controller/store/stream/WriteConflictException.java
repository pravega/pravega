/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.controller.RetryableException;

/**
 * Exception thrown when you are attempting to update a stale value.
 */
public class WriteConflictException extends RetryableException {

    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "concurrency error for path %s.";

    /**
     * Creates a new instance of WriteConflictException class.
     *
     * @param value value
     */
    public WriteConflictException(final String value) {
        super(String.format(FORMAT_STRING, value));
    }
}
