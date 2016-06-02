package com.emc.logservice.storageabstraction;

/**
 * General exception thrown by the Durable Data Log.
 */
public class DurableDataLogException extends Exception {
    /**
     * Creates a new instance of the DurableDataLogException class.
     * @param message
     */
    public DurableDataLogException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the DurableDataLogException class.
     * @param message
     * @param cause
     */
    public DurableDataLogException(String message, Throwable cause) {
        super(message, cause);
    }
}
