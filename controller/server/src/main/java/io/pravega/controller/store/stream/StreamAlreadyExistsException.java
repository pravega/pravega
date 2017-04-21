/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
