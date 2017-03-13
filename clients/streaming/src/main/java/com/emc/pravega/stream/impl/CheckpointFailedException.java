/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

public class CheckpointFailedException extends Exception {

    private static final long serialVersionUID = 1L;

    public CheckpointFailedException(String message) {
        super(message);
    }

    public CheckpointFailedException(Throwable e) {
        super(e);
    }

    public CheckpointFailedException(String message, Throwable e) {
        super(message, e);
    }
}
