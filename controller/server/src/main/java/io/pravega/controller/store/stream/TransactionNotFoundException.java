/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.store.stream;

/**
 * Exception thrown when a stream with a given name is not found in the metadata.
 */
public class TransactionNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Transaction %s not found.";

    /**
     * Creates a new instance of StreamNotFoundException class.
     *
     * @param name missing stream name
     */
    public TransactionNotFoundException(final String name) {
        super(String.format(FORMAT_STRING, name));
    }

    /**
     * Creates a new instance of StreamNotFoundException class.
     *
     * @param name  missing stream name
     * @param cause error cause
     */
    public TransactionNotFoundException(final String name, final Throwable cause) {
        super(String.format(FORMAT_STRING, name), cause);
    }
}
