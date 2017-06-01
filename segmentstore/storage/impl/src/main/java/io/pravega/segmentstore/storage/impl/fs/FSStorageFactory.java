/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.fs;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;

import java.util.concurrent.ExecutorService;

/**
 * Factory for NFS Storage adapters.
 */
public class FSStorageFactory implements StorageFactory {
    private final FSStorageConfig config;
    private final ExecutorService executor;

    /**
     * Creates a new instance of the FSStorageFactory class.
     *
     * @param config   The Configuration to use.
     * @param executor An executor to use for background operations.
     */
    public FSStorageFactory(FSStorageConfig config, ExecutorService executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
    }

    @Override
    public Storage createStorageAdapter() {
        return new FSStorage(this.config, this.executor);
    }
}
