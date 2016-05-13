package com.emc.logservice.Logs;

import com.emc.logservice.StreamingException;

/**
 * An exception that is thrown when serialization or deserialization fails.
 */
public class SerializationException extends StreamingException
{
    /**
     * Creates a new instance of the SerializationException class.
     *
     * @param source  The name of the source of the exception.
     * @param message The detail message.
     */
    public SerializationException(String source, String message)
    {
        super(combine(source, message));
    }

    /**
     * Creates a new instance of the SerializationException class.
     *
     * @param source  The name of the source of the exception.
     * @param message The detail message.
     * @param cause   Te cause of the exception.
     */
    public SerializationException(String source, String message, Throwable cause)
    {
        super(combine(source, message), cause);
    }

    /**
     * Creates a new instance of the SerializationException class.
     *
     * @param source             The name of the source of the exception.
     * @param message            The detail message.
     * @param cause              Te cause of the exception.
     * @param enableSuppression  Whether or not suppression is enabled or disabled
     * @param writableStackTrace Whether or not the stack trace should be writable
     */
    public SerializationException(String source, String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
    {
        super(combine(source, message), cause, enableSuppression, writableStackTrace);
    }

    private static String combine(String source, String message)
    {
        return String.format("%s: %s", source, message);
    }
}
