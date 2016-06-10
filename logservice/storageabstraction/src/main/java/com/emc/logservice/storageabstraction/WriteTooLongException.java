package com.emc.logservice.storageabstraction;

/**
 * Thrown when the length of a write exceeds the maximum allowed write length.
 */
public class WriteTooLongException extends DurableDataLogException {
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
     * @param cause
     */
    public WriteTooLongException(Throwable cause) {
        super("Maximum write length exceeded.", cause);
    }
}
