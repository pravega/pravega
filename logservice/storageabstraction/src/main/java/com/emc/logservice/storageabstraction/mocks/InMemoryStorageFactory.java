package com.emc.logservice.storageabstraction.mocks;

import com.emc.logservice.common.ObjectClosedException;
import com.emc.logservice.storageabstraction.Storage;
import com.emc.logservice.storageabstraction.StorageFactory;

/**
 * In-Memory mock for StorageFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemoryStorageFactory implements StorageFactory {
    private final InMemoryStorage storage = new InMemoryStorage();
    private boolean closed;

    @Override
    public Storage getStorageAdapter() {
        if (this.closed) {
            throw new ObjectClosedException(this);
        }
        return storage;
    }

    @Override
    public void close() {
        this.closed = true;
    }
}
