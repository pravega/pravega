/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.state.impl;

public class CorruptedStateException extends RuntimeException {

    /**
     * Constructor for corrupted state exception with message.
     *
     * @param message Exception description
     */
    public CorruptedStateException(String message) {
        super(message);
    }

    /**
     * Constructor for corrupted state exception with message and exception.
     *
     * @param string Exception message
     * @param e      The exception instance indicating a corrupt state and on which the new exception will build on.
     */
    public CorruptedStateException(String string, Exception e) {
        super(string, e);
    }

}
