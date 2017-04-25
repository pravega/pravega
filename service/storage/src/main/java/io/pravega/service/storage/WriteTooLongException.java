/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.storage;

/**
 * Thrown when the length of a write exceeds the maximum allowed write length.
 */
public class WriteTooLongException extends DurableDataLogException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the WriteTooLongException class.
     *
     * @param length    The length of the write.
     * @param maxLength The maximum length of the write.
     */
    public WriteTooLongException(int length, int maxLength) {
        super(String.format("Maximum write length exceeded. Max allowed %d, Actual %d.", maxLength, length));
    }

    /**
     * Creates a new instance of the WriteTooLongException class.
     *
     * @param cause The triggering cause of this exception.
     */
    public WriteTooLongException(Throwable cause) {
        super("Maximum write length exceeded.", cause);
    }
}
