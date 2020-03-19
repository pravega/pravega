/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.hdfs;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageManager;

import java.util.concurrent.Executor;

/**
 * Factory for ExtendedS3 {@link Storage} implemented using {@link ChunkStorageManager} and {@link HDFSChunkStorageProvider}.
 */
public class HDFSSimpleStorageFactory implements StorageFactory {
    private final HDFSStorageConfig config;
    private final Executor executor;

    /**
     * Creates a new instance of the HDFSStorageFactory class.
     *
     * @param config   The Configuration to use.
     * @param executor An executor to use for background operations.
     */
    public HDFSSimpleStorageFactory(HDFSStorageConfig config, Executor executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
    }

    @Override
    public Storage createStorageAdapter() {
        ChunkStorageManager storageProvider = new ChunkStorageManager(
                new HDFSChunkStorageProvider(this.executor, this.config),
                this.executor,
                SegmentRollingPolicy.NO_ROLLING);
        return storageProvider;
    }
}
