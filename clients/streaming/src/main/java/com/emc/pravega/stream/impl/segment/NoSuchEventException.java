/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl.segment;

import java.io.IOException;

/**
 * Could not retrieve an event given the event pointer.
 */
public class NoSuchEventException extends IOException {

    private static final long serialVersionUID = 1L;

    public NoSuchEventException(String message) {
        super(message);
    }
}
