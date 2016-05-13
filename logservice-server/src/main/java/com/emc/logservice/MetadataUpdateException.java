package com.emc.logservice;

/**
 * Exception that is thrown whenever the Metadata cannot be updated.
 */
public class MetadataUpdateException extends StreamingException {
    public MetadataUpdateException(String message) {
        super(message);
    }

    public MetadataUpdateException(String message, Throwable cause) {
        super(message, cause);
    }

    public MetadataUpdateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
