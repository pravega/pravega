/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.filesystem;

import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.ExecutorService;

/**
 * Factory for FileSystem {@link Storage} implemented using {@link ChunkedSegmentStorage} and {@link FileSystemChunkStorage}.
 */
@RequiredArgsConstructor
public class FileSystemSimpleStorageFactory implements SimpleStorageFactory {
    @NonNull
    private final ChunkedSegmentStorageConfig chunkedSegmentStorageConfig;

    @NonNull
    private final FileSystemStorageConfig config;

    @NonNull
    @Getter
    private final ExecutorService executor;

    @Override
    public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(containerId,
                new FileSystemChunkStorage(this.config, this.executor),
                metadataStore,
                this.executor,
                this.chunkedSegmentStorageConfig);
        return chunkedSegmentStorage;
    }

    /**
     * Creates a new instance of a Storage adapter.
     */
    @Override
    public Storage createStorageAdapter() {
        throw new UnsupportedOperationException("SimpleStorageFactory requires ChunkMetadataStore");
    }
}
