package com.emc.logservice.contracts;

/**
 * General (unchecked) Streaming Exception.
 */
public class RuntimeStreamingException extends RuntimeException {
    /**
     * Creates a new instance of the RuntimeStreamingException class.
     *
     * @param message The detail message.
     */
    public RuntimeStreamingException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the RuntimeStreamingException class.
     *
     * @param message The detail message.
     * @param cause   The cause of the exception.
     */
    public RuntimeStreamingException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance of the RuntimeStreamingException class.
     *
     * @param message            The detail message.
     * @param cause              The cause of the exception.
     * @param enableSuppression  Whether or not suppression is enabled or disabled
     * @param writableStackTrace Whether or not the stack trace should be writable
     */
    public RuntimeStreamingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
