/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.logs;

import com.emc.pravega.service.contracts.StreamingException;

/**
 * Exception that is thrown whenever the Metadata cannot be updated.
 */
public class MetadataUpdateException extends StreamingException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

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
