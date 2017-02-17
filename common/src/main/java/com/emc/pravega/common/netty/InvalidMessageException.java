/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.common.netty;

/**
 * The message or sequence of messages that occurred make no sense and must be a result of a bug.
 */
public class InvalidMessageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidMessageException() {
        super();
    }

    public InvalidMessageException(String string) {
        super(string);
    }

    public InvalidMessageException(Throwable throwable) {
        super(throwable);
    }
}
