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

import io.pravega.segmentstore.storage.chunklayer.BaseChunkStorageProvider;
import lombok.Getter;
import lombok.Setter;
import java.util.concurrent.Executor;

/**
 * Base class for InMemory mock implementations.
 * Allows to simulate ChunkStorageProvider with different supported properties.
 */
abstract public class AbstractInMemoryChunkStorageProvider extends BaseChunkStorageProvider {
    @Getter
    @Setter
    boolean shouldSupportTruncation = false;

    @Getter
    @Setter
    boolean shouldSupportAppend = true;

    @Getter
    @Setter
    boolean shouldSupportConcat = false;

    public AbstractInMemoryChunkStorageProvider(Executor executor) {
        super(executor);
    }


    @Override
    public boolean supportsTruncation() {
        return shouldSupportTruncation;
    }

    @Override
    public boolean supportsAppend() {
        return shouldSupportAppend;
    }

    @Override
    public boolean supportsConcat() {
        return shouldSupportConcat;
    }

    abstract public void addChunk(String chunkName, long length);

}
