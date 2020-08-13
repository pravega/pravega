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

import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.Executor;

/**
 * Factory for HDFS {@link Storage} implemented using {@link ChunkedSegmentStorage} and {@link HDFSChunkStorage}.
 */
@RequiredArgsConstructor
public class HDFSSimpleStorageFactory implements StorageFactory {

    @NonNull
    private final ChunkedSegmentStorageConfig chunkedSegmentStorageConfig;

    @NonNull
    private final HDFSStorageConfig config;

    @NonNull
    private final Executor executor;

    @Override
    public Storage createStorageAdapter() {
        ChunkedSegmentStorage storageProvider = new ChunkedSegmentStorage(
                new HDFSChunkStorage(this.config),
                this.executor,
                this.chunkedSegmentStorageConfig);
        return storageProvider;
    }
}
