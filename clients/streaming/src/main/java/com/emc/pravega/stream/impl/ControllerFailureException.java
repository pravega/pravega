/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

public class ControllerFailureException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ControllerFailureException(String message) {
        super(message);
    }

    public ControllerFailureException(Throwable e) {
        super(e);
    }

    public ControllerFailureException(String message, Throwable e) {
        super(message, e);
    }

}
