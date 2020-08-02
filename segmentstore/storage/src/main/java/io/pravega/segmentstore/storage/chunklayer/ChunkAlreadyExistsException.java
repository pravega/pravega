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

/**
 * Exception thrown when chunk with given name already exists.
 */
public class ChunkAlreadyExistsException extends ChunkStorageException {
    /**
     * Creates a new instance of the exception.
     *
     * @param chunkName The name of the chunk.
     * @param message   The message for this exception.
     */
    public ChunkAlreadyExistsException(String chunkName, String message) {
        super(chunkName, String.format("Chunk %s already exists - %s.", chunkName, message));
    }

    /**
     * Creates a new instance of the exception.
     *
     * @param chunkName The name of the chunk.
     * @param message   The message for this exception.
     * @param cause     The causing exception.
     */
    public ChunkAlreadyExistsException(String chunkName, String message, Throwable cause) {
        super(chunkName, String.format("Chunk %s already exists - %s.", chunkName, message), cause);
    }
}
