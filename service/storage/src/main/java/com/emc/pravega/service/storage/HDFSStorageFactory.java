package com.emc.pravega.service.storage;


import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.storage.hdfs.HDFSStorage;
import com.emc.pravega.service.storage.mocks.InMemoryStorage;

import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

public class HDFSStorageFactory implements StorageFactory {
    private final HDFSStorage storage;
    private boolean closed;

    public HDFSStorageFactory() {
        this(ForkJoinPool.commonPool());
    }

    public HDFSStorageFactory(Executor executor) {
        this.storage = new HDFSStorage(executor);
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
