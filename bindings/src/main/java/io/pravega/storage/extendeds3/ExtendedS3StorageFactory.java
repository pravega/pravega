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

import com.emc.object.s3.jersey.S3JerseyClient;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import java.util.concurrent.ExecutorService;

/**
 * Factory for ExtendedS3 Storage adapters.
 */
public class ExtendedS3StorageFactory implements StorageFactory {
    private final ExtendedS3StorageConfig config;
    private final ExecutorService executor;

    /**
     * Creates a new instance of the NFSStorageFactory class.
     *
     * @param config   The Configuration to use.
     * @param executor An executor to use for background operations.
     */
    public ExtendedS3StorageFactory(ExtendedS3StorageConfig config, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
    }

    @Override
    public Storage createStorageAdapter() {
        return new AsyncStorageWrapper(new RollingStorage(createS3Storage()), this.executor);
    }

    @Override
    public SyncStorage createSyncStorage() {
        return createS3Storage();
    }

    private ExtendedS3Storage createS3Storage() {
        S3JerseyClient client = new S3JerseyClient(config.getS3Config());
        return new ExtendedS3Storage(client, this.config);
    }
}
