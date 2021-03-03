/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.storage.chunklayer.BaseChunkStorage;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executor;

/**
 * Base class for InMemory mock implementations.
 * Allows to simulate ChunkStorage with different supported properties.
 */
@Slf4j
abstract public class AbstractInMemoryChunkStorage extends BaseChunkStorage {
    /**
     * value to return when {@link AbstractInMemoryChunkStorage#supportsTruncation()} is called.
     */
    @Getter
    @Setter
    boolean shouldSupportTruncation = true;

    /**
     * value to return when {@link AbstractInMemoryChunkStorage#supportsAppend()} is called.
     */
    @Getter
    @Setter
    boolean shouldSupportAppend = true;

    /**
     * value to return when {@link AbstractInMemoryChunkStorage#supportsConcat()} is called.
     */
    @Getter
    @Setter
    boolean shouldSupportConcat = false;

    public AbstractInMemoryChunkStorage(Executor executor) {
        super(executor);
    }

    /**
     * Gets a value indicating whether this Storage implementation supports truncate operation on underlying storage object.
     *
     * @return True or false.
     */
    @Override
    public boolean supportsTruncation() {
        return shouldSupportTruncation;
    }

    /**
     * Gets a value indicating whether this Storage implementation supports append operation on underlying storage object.
     *
     * @return True or false.
     */
    @Override
    public boolean supportsAppend() {
        return shouldSupportAppend;
    }

    /**
     * Gets a value indicating whether this Storage implementation supports merge operation on underlying storage object.
     *
     * @return True or false.
     */
    @Override
    public boolean supportsConcat() {
        return shouldSupportConcat;
    }

    /**
     * Adds a chunk of given name and length.
     *
     * @param chunkName Name of the chunk to add.
     * @param length Length of the chunk to add.
     */
    abstract public void addChunk(String chunkName, long length);
}
