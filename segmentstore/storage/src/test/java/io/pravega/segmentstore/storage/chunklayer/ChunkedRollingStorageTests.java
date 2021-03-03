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

import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.segmentstore.storage.rolling.RollingStorageTestBase;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Unit tests for  {@link ChunkedSegmentStorage} and {@link ChunkStorage} based implementation that exercise scenarios
 * for {@link io.pravega.segmentstore.storage.rolling.RollingStorage}.
 */
public abstract class ChunkedRollingStorageTests extends RollingStorageTestBase {
    private static final int CONTAINER_ID = 42;

    ChunkStorage chunkStorage;
    ChunkMetadataStore chunkMetadataStore;

    /**
     * Creates a new instance of the Storage implementation to be tested. This will be cleaned up (via close()) upon
     * test termination.
     */
    @Override
    protected Storage createStorage() throws Exception {
        useOldLayout = false;
        ScheduledExecutorService executor = executorService();
        // Initialize
        synchronized (ChunkedRollingStorageTests.class) {
            if (null == chunkStorage) {
                chunkMetadataStore = getMetadataStore();
                chunkStorage = getChunkStorage();
            }
        }
        return new ChunkedSegmentStorage(CONTAINER_ID,
                chunkStorage,
                chunkMetadataStore,
                executor,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
    }

    /**
     * Creates a ChunkStorage.
     *
     * @return ChunkStorage.
     * @throws Exception If any unexpected error occurred.
     */
    protected ChunkStorage getChunkStorage() throws Exception {
        return new InMemoryChunkStorage(executorService());
    }

    /**
     * Creates a ChunkMetadataStore.
     *
     * @return ChunkMetadataStore
     * @throws Exception If any unexpected error occurred.
     */
    protected ChunkMetadataStore getMetadataStore() throws Exception {
        return new InMemoryMetadataStore(executorService());
    }

    @Override
    public void testListSegmentsWithOneSegment() {
    }

    @Override
    public void testListSegmentsNextNoSuchElementException() {
    }
}
