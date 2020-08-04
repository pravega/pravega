/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.segmentstore.storage.noop.NoOpStorageFactory;
import io.pravega.segmentstore.storage.noop.StorageExtraConfig;
import org.junit.Test;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.assertTrue;

public class StorageLoaderTest {

    private StorageFactory expectedFactory;

    @Test
    public void testNoOpWithWithInMemoryStorage() throws Exception {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                .builder()
                .include(StorageExtraConfig.builder()
                        .with(StorageExtraConfig.STORAGE_NO_OP_MODE, true))
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1)
                        .with(ServiceConfig.STORAGE_IMPLEMENTATION, ServiceConfig.StorageType.INMEMORY));

        ServiceBuilder builder = ServiceBuilder.newInMemoryBuilder(configBuilder.build())
                .withStorageFactory(setup -> {
                    StorageLoader loader = new StorageLoader();
                    expectedFactory = loader.load(setup, "INMEMORY", executor);
                    return expectedFactory;
                });
        builder.initialize();
        assertTrue(expectedFactory instanceof NoOpStorageFactory);
        builder.close();

        configBuilder
                .include(StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_NO_OP_MODE, false));

        builder = ServiceBuilder.newInMemoryBuilder(configBuilder.build())
                .withStorageFactory(setup -> {
                    StorageLoader loader = new StorageLoader();
                    expectedFactory = loader.load(setup, "INMEMORY", executor);
                    return expectedFactory;
                });
        builder.initialize();
        assertTrue(expectedFactory instanceof InMemoryStorageFactory);
        builder.close();
    }
}