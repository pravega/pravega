/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.bindings.filesystem;

import com.google.common.base.Preconditions;
import io.pravega.common.util.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;

import java.util.concurrent.ExecutorService;

/**
 * Factory for file system Storage adapters.
 */
public class FileSystemStorageFactory implements StorageFactory {
    private final FileSystemStorageConfig config;
    private final ExecutorService executor;

    /**
     * Creates a new instance of the FileSystemStorageFactory class.
     *
     * @param config   The Configuration to use.
     * @param executor An executor to use for background operations.
     */
    public FileSystemStorageFactory(ServiceBuilderConfig config, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config.getConfig(FileSystemStorageConfig::builder);
        this.executor = executor;
    }

    @Override
    public Storage createStorageAdapter() {
        return new FileSystemStorage(this.config, this.executor);
    }
}
