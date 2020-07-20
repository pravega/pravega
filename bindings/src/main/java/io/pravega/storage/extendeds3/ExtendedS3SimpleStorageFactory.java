/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import com.emc.object.s3.S3Client;
import com.emc.object.s3.S3Config;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;

import java.util.concurrent.ExecutorService;

/**
 * Factory for ExtendedS3 {@link Storage} implemented using {@link ChunkedSegmentStorage} and {@link ExtendedS3ChunkStorage}.
 */
public class ExtendedS3SimpleStorageFactory implements StorageFactory {
    private final ExtendedS3StorageConfig config;
    private final ExecutorService executor;

    /**
     * Creates a new instance of the {@link ExtendedS3SimpleStorageFactory} class.
     *
     * @param config   The Configuration to use.
     * @param executor An executor to use for background operations.
     */
    public ExtendedS3SimpleStorageFactory(ExtendedS3StorageConfig config, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
    }

    @Override
    public Storage createStorageAdapter() {
        ChunkedSegmentStorage storageProvider = new ChunkedSegmentStorage(
                new ExtendedS3ChunkStorage(createS3Client(), this.config),
                this.executor,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        return storageProvider;
    }

    private S3Client createS3Client() {
        S3Config s3Config = new S3Config(config.getS3Config());
        S3JerseyClient client = new S3JerseyClient(s3Config);
        return client;
    }
}
