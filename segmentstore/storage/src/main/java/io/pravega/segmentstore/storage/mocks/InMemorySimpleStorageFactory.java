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

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * In-Memory mock for StorageFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemorySimpleStorageFactory implements StorageFactory {
    protected ScheduledExecutorService executor;

    private Storage singletonStorage;
    private boolean reuseStorage;

    public InMemorySimpleStorageFactory(ScheduledExecutorService executor, boolean reuseStorage) {
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.reuseStorage = reuseStorage;
    }

    @Override
    public Storage createStorageAdapter() {
        synchronized (this) {
            if (null != singletonStorage) {
                return singletonStorage;
            }
            Storage storage = newStorage(executor, new InMemoryChunkStorage());
            if (reuseStorage) {
                singletonStorage = storage;
            }
            return storage;
        }
    }

    /**
     * Creates a new InMemory Storage, without a rolling wrapper.
     *
     * @param executor     An Executor to use for async operations.
     * @param chunkStorage ChunkStorage to use.
     * @return A new InMemoryStorage.
     */
    static Storage newStorage(Executor executor, ChunkStorage chunkStorage) {
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(
                chunkStorage,
                executor,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        chunkedSegmentStorage.initialize(1);
        return chunkedSegmentStorage;
    }
}
