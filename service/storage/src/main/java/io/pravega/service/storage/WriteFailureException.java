/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.storage;

/**
 * Exception that is thrown whenever a general Write Failure occurred.
 */
public class WriteFailureException extends DurableDataLogException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the WriteFailureException class.
     *
     * @param message The message to set.
     */
    public WriteFailureException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the WriteFailureException class.
     *
     * @param message The message to set.
     * @param cause   The triggering cause of this exception.
     */
    public WriteFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
