/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.service.storage.impl.hdfs;

import io.pravega.service.storage.Storage;
import io.pravega.service.storage.StorageFactory;
import com.google.common.base.Preconditions;
import java.util.concurrent.Executor;

/**
 * Factory for HDFS Storage adapters.
 */
public class HDFSStorageFactory implements StorageFactory {
    private final HDFSStorageConfig config;
    private final Executor executor;

    /**
     * Creates a new instance of the HDFSStorageFactory class.
     *
     * @param config   The Configuration to use.
     * @param executor An executor to use for background operations.
     */
    public HDFSStorageFactory(HDFSStorageConfig config, Executor executor) {
        Preconditions.checkNotNull(config, "config");
        Preconditions.checkNotNull(executor, "executor");
        this.config = config;
        this.executor = executor;
    }

    @Override
    public Storage createStorageAdapter() {
        return new HDFSStorage(this.config, this.executor);
    }
}
