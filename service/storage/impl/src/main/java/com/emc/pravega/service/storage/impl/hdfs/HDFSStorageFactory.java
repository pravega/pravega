package com.emc.pravega.service.storage.impl.hdfs;


import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.storage.Storage;
import com.emc.pravega.service.storage.StorageFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public class HDFSStorageFactory implements StorageFactory {
    private final HDFSStorage storage;
    private boolean closed;


    public HDFSStorageFactory(HDFSStorageConfig serviceBuilderConfig, Executor executor) {
        this.storage = new HDFSStorage(serviceBuilderConfig,executor);
    }

    public HDFSStorageFactory(HDFSStorageConfig serviceBuilderConfig) {
        this(serviceBuilderConfig,ForkJoinPool.commonPool());

    }

    @Override
    public Storage getStorageAdapter() {
        Exceptions.checkNotClosed(this.closed, this);
        return storage;
    }

    @Override
    public void close() {
        this.closed = true;
    }
}
