/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

/**
 * Exception thrown when data already exists in metadata.
 */
public class DataExistsException extends RuntimeException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Data %s already exists.";

    /**
     * Creates a new instance of DataExistsException class.
     *
     * @param name data identifier
     */
    public DataExistsException(final String name) {
        super(String.format(FORMAT_STRING, name));
    }

    /**
     * Creates a new instance of DataExistsException class.
     *
     * @param name  data identifier
     * @param cause error cause
     */
    public DataExistsException(final String name, final Throwable cause) {
        super(String.format(FORMAT_STRING, name), cause);
    }
}
