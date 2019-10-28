/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.noop;

import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import org.junit.Before;
import org.junit.Ignore;

public class NoOpStorageWriteReadTests extends StorageTestBase {

    private SyncStorage systemStorage;
    private SyncStorage userStorage;
    private StorageExtraConfig config;

    @Before
    public void setUp() {
        systemStorage = new InMemoryStorageFactory(executorService()).createSyncStorage();
        userStorage = new InMemoryStorageFactory(executorService()).createSyncStorage();
        config = StorageExtraConfig.builder().with(StorageExtraConfig.STORAGE_NO_OP_MODE, true).build();
    }

    @Override
    protected Storage createStorage() {
        return new AsyncStorageWrapper(new NoOpStorage(config, systemStorage, userStorage), executorService());
    }

    @Override
    @Ignore
    public void testFencing() throws Exception {
    }

}
