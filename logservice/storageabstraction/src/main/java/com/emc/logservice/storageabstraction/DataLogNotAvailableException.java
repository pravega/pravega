package com.emc.logservice.storageabstraction;

/**
 * Exception that is thrown whenever the Durable Data Log (or one its components) is unavailable.
 */
public class DataLogNotAvailableException extends Exception {
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
