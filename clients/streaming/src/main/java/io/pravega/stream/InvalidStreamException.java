/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream;

public class InvalidStreamException extends RuntimeException {

    public InvalidStreamException(String message) {
        super(message);
    }
    
    public InvalidStreamException(Throwable e) {
        super(e);
    }
    
    public InvalidStreamException(String message, Throwable e) {
        super(message, e);
    }
}
