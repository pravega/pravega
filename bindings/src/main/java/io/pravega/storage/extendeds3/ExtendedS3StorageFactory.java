/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import com.emc.object.s3.S3Config;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.google.common.base.Preconditions;
import io.pravega.common.ConfigSetup;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory for ExtendedS3 Storage adapters.
 */
public class ExtendedS3StorageFactory implements StorageFactory {
    private AtomicReference<ExtendedS3StorageConfig> config;
    private AtomicReference<ExecutorService> executor;

    /**
     * Creates a new instance of the NFSStorageFactory class.
     *
     * @param config   The Configuration to use.
     * @param executor An executor to use for background operations.
     */
    public ExtendedS3StorageFactory(ExtendedS3StorageConfig config, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = new AtomicReference<>(config);
        this.executor = new AtomicReference<>(executor);
    }

    public ExtendedS3StorageFactory() {
        this.config = new AtomicReference<>();
        this.executor = new AtomicReference<>();
    }

    @Override
    public Storage createStorageAdapter() {
        S3Config s3Config = new S3Config(config.get().getUrl())
                .withIdentity(config.get().getAccessKey())
                .withSecretKey(config.get().getSecretKey())
                .withNamespace(config.get().getNamespace());

        S3JerseyClient client = new S3JerseyClient(s3Config);
        ExtendedS3Storage s = new ExtendedS3Storage(client, this.config.get());
        return new AsyncStorageWrapper(new RollingStorage(s), this.executor.get());
    }

    @Override
    public String getName() {
        return "EXTENDEDS3";
    }

    @Override
    public synchronized void initialize(ConfigSetup setup, ScheduledExecutorService executor) {
        this.config.set(setup.getConfig(ExtendedS3StorageConfig::builder));
        this.executor.set(executor);
    }
}
