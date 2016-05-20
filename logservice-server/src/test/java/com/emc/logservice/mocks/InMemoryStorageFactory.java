package com.emc.logservice.mocks;

import com.emc.logservice.Storage;
import com.emc.logservice.StorageFactory;

/**
 * Created by padura on 5/18/16.
 */
public class InMemoryStorageFactory implements StorageFactory {
    private final InMemoryStorage storage = new InMemoryStorage();

    @Override
    public Storage getStorageAdapter() {
        return storage;
    }
}
