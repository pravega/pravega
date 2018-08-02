/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.hdfs;

import com.google.common.base.Preconditions;
import io.pravega.common.ConfigSetup;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory for HDFS Storage adapters.
 */
public class HDFSStorageFactory implements StorageFactory {
    private AtomicReference<HDFSStorageConfig> config;
    private AtomicReference<Executor> executor;

    /**
     * Creates a new instance of the HDFSStorageFactory class.
     *
     * @param config   The Configuration to use.
     * @param executor An executor to use for background operations.
     */
    public HDFSStorageFactory(HDFSStorageConfig config, Executor executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = new AtomicReference<>(config);
        this.executor = new AtomicReference<>(executor);
    }

    public HDFSStorageFactory() {
        this.config = new AtomicReference<>();
        this.executor = new AtomicReference<>();
    }

    @Override
    public synchronized Storage createStorageAdapter() {
        HDFSStorage s = new HDFSStorage(this.config.get());
        return new AsyncStorageWrapper(new RollingStorage(s), this.executor.get());
    }

    @Override
    public String getName() {
        return "HDFS";
    }

    @Override
    public synchronized void initialize(ConfigSetup setup, ScheduledExecutorService executor) {
        this.config.set(setup.getConfig(HDFSStorageConfig::builder));
        this.executor.set(executor);
    }
}
