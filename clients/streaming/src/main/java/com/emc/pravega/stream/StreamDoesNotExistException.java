/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream;

public class StreamDoesNotExistException extends RuntimeException {

    public StreamDoesNotExistException(String message) {
        super(message);
    }
    
    public StreamDoesNotExistException(Throwable e) {
        super(e);
    }
    
    public StreamDoesNotExistException(String message, Throwable e) {
        super(message, e);
    }
}
