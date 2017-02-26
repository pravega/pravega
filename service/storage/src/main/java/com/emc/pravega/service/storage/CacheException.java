/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.storage;

/**
 * Exception that is thrown whenever a Cache operation could not be completed.
 */
public class CacheException extends RuntimeException {
    /**
     * Creates a new instance of the CacheException class.
     *
     * @param message The message to use.
     */
    public CacheException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the CacheException class.
     *
     * @param message The message to use.
     * @param cause   The causing exception.
     */
    public CacheException(String message, Throwable cause) {
        super(message, cause);
    }
}
