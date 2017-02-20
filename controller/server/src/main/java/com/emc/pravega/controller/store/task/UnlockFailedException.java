/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.task;

/**
 * Unlock failed exception.
 */
public class UnlockFailedException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Failed unlocking resource %s.";

    /**
     * Creates a new instance of StreamAlreadyExistsException class.
     *
     * @param name duplicate stream name
     */
    public UnlockFailedException(final String name) {
        super(String.format(FORMAT_STRING, name));
    }

    /**
     * Creates a new instance of StreamAlreadyExistsException class.
     *
     * @param name  duplicate stream name
     * @param cause error cause
     */
    public UnlockFailedException(final String name, final Throwable cause) {
        super(String.format(FORMAT_STRING, name), cause);
    }
}
