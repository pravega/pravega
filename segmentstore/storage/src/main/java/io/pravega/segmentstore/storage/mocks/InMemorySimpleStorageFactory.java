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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageManager;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageProvider;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/**
 * In-Memory mock for StorageFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemorySimpleStorageFactory implements StorageFactory, AutoCloseable {
    @VisibleForTesting
    protected ScheduledExecutorService executor;

    private Storage singletonStorage;
    private ChunkStorageProvider singletonChunkStorageProvider;
    private boolean reuseStorage;
    public InMemorySimpleStorageFactory(ScheduledExecutorService executor) {
        this.executor = Preconditions.checkNotNull(executor, "executor");
    }

    public InMemorySimpleStorageFactory() {
    }

    public InMemorySimpleStorageFactory(ScheduledExecutorService executor, boolean reuseStorage) {
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.reuseStorage = reuseStorage;
    }

    public InMemorySimpleStorageFactory(ScheduledExecutorService executor, Storage storage) {
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.singletonStorage = Preconditions.checkNotNull(storage, "Storage");
        this.reuseStorage = true;
    }

    public InMemorySimpleStorageFactory(ScheduledExecutorService executor, ChunkStorageProvider chunkStorageProvider) {
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.singletonChunkStorageProvider = Preconditions.checkNotNull(chunkStorageProvider, "chunkStorageProvider");
        this.reuseStorage = false;
    }

    @Override
    public Storage createStorageAdapter() {
        synchronized (this) {
            if (reuseStorage) {
                if (null != singletonStorage) {
                    return singletonStorage;
                }
                singletonStorage = getStorage();
                return singletonStorage;
            }
            return getStorage();
        }
    }

    private Storage getStorage() {
        if (null == singletonChunkStorageProvider) {
            return newStorage(executor);
        } else {
            return newStorage(executor, singletonChunkStorageProvider);
        }
    }

    @Override
    public void close() {
        // ?
    }

    /**
     * Creates a new InMemory Storage, without a rolling wrapper.
     *
     * @param executor An Executor to use for async operations.
     * @return A new InMemoryStorage.
     */
    @VisibleForTesting
    public static Storage newStorage(Executor executor) {
        return newStorage(executor,  new InMemoryChunkStorageProvider(executor));
    }

    /**
     * Creates a new InMemory Storage, without a rolling wrapper.
     *
     * @param executor An Executor to use for async operations.
     * @param chunkStorageProvider  ChunkStorageProvider to use.
     * @return A new InMemoryStorage.
     */
    @VisibleForTesting
    public static Storage newStorage(Executor executor, ChunkStorageProvider chunkStorageProvider) {
        //TableStore tableStore = new InMemoryTableStore(executor);
        //tableStore.createSegment("InMemoryStorageFactory", null).join();
        ChunkStorageManager chunkStorageManager = new ChunkStorageManager(
                chunkStorageProvider,
                //new TableBasedMetadataStore("InMemoryStorageFactory", tableStore),
                executor,
                SegmentRollingPolicy.NO_ROLLING);
        chunkStorageManager.initialize(1);
        return chunkStorageManager;
    }
}
