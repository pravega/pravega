package com.emc.logservice.storageabstraction;

/**
 * Exception that is thrown whenever it is detected that the current DataLog is not the primary writer to the log anymore.
 * This means one of two things:
 * <ul>
 * <li> We lost the exclusive write lock to the log, so we do not have the right to write to it anymore.
 * <li> We were never able to acquire the exclusive write lock to the log, most likely because we were in a race with
 * some other requester and we lost.
 * </ul>
 */
public class DataLogWriterNotPrimaryException extends DurableDataLogException {
    /**
     * Creates a new instance of the DataLogWriterNotPrimaryException class.
     *
     * @param message
     */
    public DataLogWriterNotPrimaryException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the DataLogNotAvailable class.
     *
     * @param message
     * @param cause
     */
    public DataLogWriterNotPrimaryException(String message, Throwable cause) {
        super(message, cause);
    }
}
