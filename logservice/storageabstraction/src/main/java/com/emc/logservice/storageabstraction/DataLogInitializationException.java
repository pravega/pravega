package com.emc.logservice.storageabstraction;

/**
 * Exception that is thrown whenever an initialization problem occurred with the DurableDataLog.
 */
public class DataLogInitializationException extends DurableDataLogException  {
    /**
     * Creates a new instance of the DataLogNotAvailable class.
     * @param message
     */
    public DataLogInitializationException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the DataLogNotAvailable class.
     * @param message
     * @param cause
     */
    public DataLogInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
