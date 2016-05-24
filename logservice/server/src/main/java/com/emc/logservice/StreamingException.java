package com.emc.logservice;

/**
 * General Streaming Exception.
 */
public class StreamingException extends Exception {
    /**
     * Creates a new instance of the StreamingException class.
     *
     * @param message The detail message.
     */
    public StreamingException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the StreamingException class.
     *
     * @param message The detail message.
     * @param cause   The cause of the exception.
     */
    public StreamingException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance of the StreamingException class.
     *
     * @param message            The detail message.
     * @param cause              The cause of the exception.
     * @param enableSuppression  Whether or not suppression is enabled or disabled
     * @param writableStackTrace Whether or not the stack trace should be writable
     */
    public StreamingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
