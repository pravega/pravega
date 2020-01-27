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
 * Exception that is thrown whenever it is detected that the current DataLog is not the primary writer to the log anymore.
 * This means one of two things:
 * <ul>
 * <li> We lost the exclusive write lock to the log, so we do not have the right to write to it anymore.
 * <li> We were never able to acquire the exclusive write lock to the log, most likely because we were in a race with
 * some other requester and we lost.
 * </ul>
 */
public class DataLogWriterNotPrimaryException extends DurableDataLogException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the DataLogWriterNotPrimaryException class.
     *
     * @param message The message to set.
     */
    public DataLogWriterNotPrimaryException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the DataLogNotAvailable class.
     *
     * @param message The message to set.
     * @param cause   The triggering cause of this exception.
     */
    public DataLogWriterNotPrimaryException(String message, Throwable cause) {
        super(message, cause);
    }
}
