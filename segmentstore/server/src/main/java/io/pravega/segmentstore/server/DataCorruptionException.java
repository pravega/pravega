/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.contracts.StreamingException;
import lombok.Getter;

/**
 * Exception that is thrown whenever we detect an unrecoverable data corruption.
 * Usually, after this is thrown, our only resolution may be to suspend processing in the container or completely bring it offline.
 */
public class DataCorruptionException extends StreamingException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Gets an array of objects that may contain additional context-related information.
     */
    @Getter
    private final Object[] additionalData;

    /**
     * Creates a new instance of the DataCorruptionException class.
     *
     * @param message        The message for the exception.
     * @param additionalData An array of objects that contain additional debugging information.
     */
    public DataCorruptionException(String message, Object... additionalData) {
        this(message, null, additionalData);
    }

    /**
     * Creates a new instance of the DataCorruptionException class.
     *
     * @param message        The message for the exception.
     * @param cause          The causing exception.
     * @param additionalData An array of objects that contain additional debugging information.
     */
    public DataCorruptionException(String message, Throwable cause, Object... additionalData) {
        super(message, cause);
        this.additionalData = additionalData;
    }

    public DataCorruptionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.additionalData = null;
    }
}
