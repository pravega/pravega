/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.chunklayer;

import lombok.Getter;

import java.io.IOException;

/**
 * Generic exception related to chunk storage operations.
 */
public class ChunkStorageException extends IOException {
    @Getter
    private final String chunkName;

    /**
     * Creates a new instance of the exception.
     *
     * @param chunkName The name of the chunk.
     * @param message   The message for this exception.
     */
    public ChunkStorageException(String chunkName, String message) {
        super(message);
        this.chunkName = chunkName;
    }

    /**
     * Creates a new instance of the exception.
     *
     * @param chunkName The name of the chunk.
     * @param message   The message for this exception.
     * @param cause     The causing exception.
     */
    public ChunkStorageException(String chunkName, String message, Throwable cause) {
        super(message, cause);
        this.chunkName = chunkName;
    }
}
