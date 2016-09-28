package com.emc.pravega.service.storage;


import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.service.storage.hdfs.HDFSStorage;
import com.emc.pravega.service.storage.mocks.InMemoryStorage;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public class HDFSStorageFactory implements StorageFactory {
    private final HDFSStorage storage;
    private boolean closed;


    public HDFSStorageFactory(ServiceBuilderConfig serviceBuilderConfig,Executor executor) {
        this.storage = new HDFSStorage(serviceBuilderConfig,executor);
    }

    public HDFSStorageFactory(ServiceBuilderConfig serviceBuilderConfig) {
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
