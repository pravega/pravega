package com.emc.logservice.storageabstraction;

/**
 * Exception that is thrown whenever a Write failed due to a bad offset.
 */
public class BadOffsetException extends WriteFailureException {
    /**
     * Creates a new instance of the BadOffsetException class.
     *
     * @param message
     */
    public BadOffsetException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the BadOffsetException class.
     *
     * @param message
     * @param cause
     */
    public BadOffsetException(String message, Throwable cause) {
        super(message, cause);
    }
}
