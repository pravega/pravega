/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.hdfs;

import io.pravega.common.health.processor.HealthRequestProcessor;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import com.google.common.base.Preconditions;
import java.util.concurrent.Executor;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory for HDFS Storage adapters.
 */
@Slf4j
public class HDFSStorageFactory implements StorageFactory {
    private final HDFSStorageConfig config;
    private final Executor executor;
    private HealthRequestProcessor healthRequestProcessor;

    /**
     * Creates a new instance of the HDFSStorageFactory class.
     *
     * @param config   The Configuration to use.
     * @param executor An executor to use for background operations.
     * @param healthRequestProcessor the processor for sending health requests to Storage implementation.
     */
    public HDFSStorageFactory(HDFSStorageConfig config, Executor executor, HealthRequestProcessor healthRequestProcessor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
        this.healthRequestProcessor = healthRequestProcessor;
    }

    @Override
    public Storage createStorageAdapter() {
        HDFSStorage hdfsStorage = new HDFSStorage(this.config, this.executor);
        this.healthRequestProcessor.registerHealthProcessor("segmentstore/storage/hdfs", hdfsStorage);
        return hdfsStorage;
    }
}
