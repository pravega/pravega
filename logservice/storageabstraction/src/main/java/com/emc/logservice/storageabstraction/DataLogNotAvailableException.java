package com.emc.logservice.storageabstraction;

/**
 * Exception that is thrown whenever the Durable Data Log (or one its components) is unavailable.
 * This generally indicates a temporary unavailability, which can go away if the operation is retried.
 */
public class DataLogNotAvailableException extends DurableDataLogException {
    /**
     * Creates a new instance of the DataLogNotAvailable class.
     * @param message
     */
    public DataLogNotAvailableException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the DataLogNotAvailable class.
     * @param message
     * @param cause
     */
    public DataLogNotAvailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
