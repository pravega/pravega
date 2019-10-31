/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

/**
 * Exception that is thrown whenever a Cache operation could not be completed.
 */
public class CacheException extends RuntimeException {
    private static final long serialVersionUID = 1L;
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
