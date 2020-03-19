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

import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorageProvider;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.segmentstore.storage.rolling.RollingStorageTestBase;

import java.util.concurrent.Executor;

/**
 * Unit tests for  {@link ChunkStorageManager} and {@link ChunkStorageProvider} based implementation that exercise scenarios
 * for {@link io.pravega.segmentstore.storage.rolling.RollingStorage}.
 */
public abstract class ChunkManagerRollingTests extends RollingStorageTestBase {
    ChunkStorageProvider chunkStorageProvider;
    ChunkMetadataStore chunkMetadataStore;

    /**
     * Creates a new instance of the Storage implementation to be tested. This will be cleaned up (via close()) upon
     * test termination.
     */
    @Override
    protected Storage createStorage() throws Exception {
        useOldLayout = false;
        Executor executor = executorService();
        // Initialize
        synchronized (ChunkManagerRollingTests.class) {
            if (null == chunkStorageProvider) {
                chunkMetadataStore = getMetadataStore();
                chunkStorageProvider = getChunkStorage(executor);
            }
        }
        return new ChunkStorageManager(chunkStorageProvider,
                chunkMetadataStore,
                executor,
                SegmentRollingPolicy.NO_ROLLING);
    }

    /**
     * Creates a ChunkStorageProvider.
     * @param executor Storage executor to use.
     * @return ChunkStorageProvider.
     * @throws Exception If any unexpected error occurred.
     */
    protected ChunkStorageProvider getChunkStorage(Executor executor) throws Exception {
        return new InMemoryChunkStorageProvider(executor);
    }

    /**
     * Creates a ChunkMetadataStore.
     * @return ChunkMetadataStore
     * @throws Exception If any unexpected error occurred.
     */
    protected ChunkMetadataStore getMetadataStore()  throws Exception {
        return new InMemoryMetadataStore();
    }
}
