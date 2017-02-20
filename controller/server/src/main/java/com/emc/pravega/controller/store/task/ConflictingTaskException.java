/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.controller.store.task;

/**
 * Conflict exception.
 */
public class ConflictingTaskException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Conflicting task exception for resource %s.";

    /**
     * Creates a new instance of ConflictingTaskException class.
     *
     * @param name resource on which lock failed
     */
    public ConflictingTaskException(final String name) {
        super(String.format(FORMAT_STRING, name));
    }

    /**
     * Creates a new instance of ConflictingTaskException class.
     *
     * @param name  resource on which conflicting task
     * @param cause error cause
     */
    public ConflictingTaskException(final String name, final Throwable cause) {
        super(String.format(FORMAT_STRING, name), cause);
    }
}
