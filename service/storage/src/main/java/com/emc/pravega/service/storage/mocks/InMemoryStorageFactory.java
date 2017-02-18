/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.storage.mocks;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.storage.StorageFactory;
import com.emc.pravega.service.storage.TruncateableStorage;

import java.util.concurrent.ScheduledExecutorService;

/**
 * In-Memory mock for StorageFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemoryStorageFactory implements StorageFactory {
    private final InMemoryStorage storage;
    private boolean closed;

    public InMemoryStorageFactory(ScheduledExecutorService executor) {
        this.storage = new InMemoryStorage(executor);
    }

    @Override
    public TruncateableStorage getStorageAdapter() {
        Exceptions.checkNotClosed(this.closed, this);
        return storage;
    }

    @Override
    public void close() {
        this.closed = true;
    }
}
