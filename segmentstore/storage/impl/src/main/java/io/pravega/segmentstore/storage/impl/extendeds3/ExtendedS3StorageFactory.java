/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.extendeds3;

import com.emc.object.s3.S3Config;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.google.common.base.Preconditions;
import io.pravega.common.health.processor.HealthRequestProcessor;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;

import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory for ExtendedS3 Storage adapters.
 */
@Slf4j
public class ExtendedS3StorageFactory implements StorageFactory {
    private final ExtendedS3StorageConfig config;
    private final ExecutorService executor;
    private HealthRequestProcessor healthRequestProcessor;

    /**
     * Creates a new instance of the NFSStorageFactory class.
     *
     * @param config   The Configuration to use.
     * @param executor An executor to use for background operations.
     * @param healthRequestProcessor the processor for sending health requests to Storage implementation.
     */
    public ExtendedS3StorageFactory(ExtendedS3StorageConfig config, ExecutorService executor, HealthRequestProcessor healthRequestProcessor) {
        this.healthRequestProcessor = healthRequestProcessor;
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
        this.healthRequestProcessor = healthRequestProcessor;
    }

    @Override
    public Storage createStorageAdapter() {
        S3Config s3Config = new S3Config(config.getUrl())
                .withIdentity(config.getAccessKey())
                .withSecretKey(config.getSecretKey());

        S3JerseyClient client = new S3JerseyClient(s3Config);
        ExtendedS3Storage storage = new ExtendedS3Storage(client, this.config, this.executor);
        healthRequestProcessor.registerHealthProcessor("segmentstore/storage/s3", storage);
        return storage;
    }
}
