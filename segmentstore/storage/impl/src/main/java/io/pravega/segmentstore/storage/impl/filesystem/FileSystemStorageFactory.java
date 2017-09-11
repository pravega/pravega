/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.filesystem;

import com.google.common.base.Preconditions;
import io.pravega.common.health.processor.HealthRequestProcessor;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;

import java.util.concurrent.ExecutorService;
import lombok.extern.slf4j.Slf4j;

/**
 * Factory for file system Storage adapters.
 */
@Slf4j
public class FileSystemStorageFactory implements StorageFactory {
    private final FileSystemStorageConfig config;
    private final ExecutorService executor;
    private HealthRequestProcessor healthRequestProcessor;

    /**
     * Creates a new instance of the FileSystemStorageFactory class.
     *
     * @param config   The Configuration to use.
     * @param executor An executor to use for background operations.
     * @param healthRequestProcessor the processor for sending health requests to Storage implementation.
     */
    public FileSystemStorageFactory(FileSystemStorageConfig config, ExecutorService executor, HealthRequestProcessor healthRequestProcessor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
        this.healthRequestProcessor = healthRequestProcessor;
    }

    @Override
    public Storage createStorageAdapter() {
        FileSystemStorage fileSystemStorage = new FileSystemStorage(this.config, this.executor);
        this.healthRequestProcessor.registerHealthProcessor("segmentstore/storage/fs", fileSystemStorage);
        return fileSystemStorage;
    }
}
