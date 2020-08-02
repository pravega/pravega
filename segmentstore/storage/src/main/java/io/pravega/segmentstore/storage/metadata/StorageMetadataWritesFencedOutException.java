/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.metadata;

/**
 * Exception thrown when current instance is write fenced out.
 */
public class StorageMetadataWritesFencedOutException extends StorageMetadataException {
    /**
     * Creates a new instance of the exception.
     *
     * @param message The message for this exception.
     */
    public StorageMetadataWritesFencedOutException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the exception.
     *
     * @param message The message for this exception.
     * @param cause   The causing exception.
     */
    public StorageMetadataWritesFencedOutException(String message, Throwable cause) {
        super(message, cause);
    }
}
