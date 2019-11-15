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

import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import org.junit.Test;

import java.util.concurrent.ScheduledThreadPoolExecutor;

public class NoOpStorageFactoryTest {

    @Test(expected = UnsupportedOperationException.class)
    public void testCreateSyncStorage() {
        StorageExtraConfig config = StorageExtraConfig.builder().build();
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        StorageFactory factory = new NoOpStorageFactory(config, executor, new InMemoryStorageFactory(executor), null);
        factory.createSyncStorage();
    }
}
