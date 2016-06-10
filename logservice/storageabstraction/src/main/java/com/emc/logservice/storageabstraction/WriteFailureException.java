package com.emc.logservice.storageabstraction;

/**
 * Exception that is thrown whenever a general Write Failure occurred.
 */
public class WriteFailureException extends DurableDataLogException {
    /**
     * Creates a new instance of the WriteFailureException class.
     *
     * @param message
     */
    public WriteFailureException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the WriteFailureException class.
     *
     * @param message
     * @param cause
     */
    public WriteFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
