/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.task;

/**
 * Exception thrown on finding a duplicate task annotation: combination of method name and version.
 */
public class DuplicateTaskAnnotationException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Duplicate task annotation for method %s version %s.";

    /**
     * Creates a new instance of DuplicateTaskAnnotationException class.
     *
     * @param method  method name
     * @param version method version
     */
    public DuplicateTaskAnnotationException(final String method, final String version) {
        super(String.format(FORMAT_STRING, method, version));
    }

    /**
     * Creates a new instance of DuplicateTaskAnnotationException class.
     *
     * @param method  method name
     * @param version method version
     * @param cause   error cause
     */
    public DuplicateTaskAnnotationException(final String method, final String version, final Throwable cause) {
        super(String.format(FORMAT_STRING, method, version), cause);
    }

}
