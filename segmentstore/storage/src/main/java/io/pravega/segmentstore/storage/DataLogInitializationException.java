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
 * Exception that is thrown whenever an initialization problem occurred with the DurableDataLog.
 */
public class DataLogInitializationException extends DurableDataLogException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the DataLogNotAvailable class.
     *
     * @param message The message to set.
     */
    public DataLogInitializationException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the DataLogNotAvailable class.
     *
     * @param message The message to set.
     * @param cause   The triggering cause of this exception.
     */
    public DataLogInitializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
