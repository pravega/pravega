package com.emc.logservice;

/**
 * Exception that is thrown whenever we detect an unrecoverable data corruption.
 * Usually, after this is thrown, our only resolution may be to suspend processing in the container or completely bring it offline.
 */
public class DataCorruptionException extends StreamingException
{
    public DataCorruptionException(String message)
    {
        super(message);
    }

    public DataCorruptionException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public DataCorruptionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
