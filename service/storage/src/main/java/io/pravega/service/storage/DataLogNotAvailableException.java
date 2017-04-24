/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.storage;

/**
 * Exception that is thrown whenever the Durable Data Log (or one its components) is unavailable.
 * This generally indicates a temporary unavailability, which can go away if the operation is retried.
 */
public class DataLogNotAvailableException extends DurableDataLogException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the DataLogNotAvailable class.
     *
     * @param message The message to set.
     */
    public DataLogNotAvailableException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the DataLogNotAvailable class.
     *
     * @param message The message to set.
     * @param cause   The triggering cause of this exception.
     */
    public DataLogNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
