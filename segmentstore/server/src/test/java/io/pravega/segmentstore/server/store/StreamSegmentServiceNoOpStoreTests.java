/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.store;

import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.segmentstore.storage.noop.NoOpStorageFactory;
import io.pravega.segmentstore.storage.noop.StorageExtraConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Unit tests for the StreamSegmentService using NoOpStorage.
 */
public class StreamSegmentServiceNoOpStoreTests extends StreamSegmentStoreTestBase {
    private InMemoryStorageFactory underlyingStorageFactory;
    private NoOpStorageFactory storageFactory;
    private InMemoryDurableDataLogFactory durableDataLogFactory;

    @Before
    public void setUp() {
        ScheduledExecutorService executor = executorService();
        this.underlyingStorageFactory = new InMemoryStorageFactory(executor);

        StorageExtraConfig config = StorageExtraConfig.builder().with(StorageExtraConfig.STORAGE_NO_OP_MODE, true).build();
        this.storageFactory = new NoOpStorageFactory(config, executor, underlyingStorageFactory);
        this.durableDataLogFactory = new PermanentDurableDataLogFactory(executorService());
    }

    @After
    public void tearDown() {
        if (this.durableDataLogFactory != null) {
            this.durableDataLogFactory.close();
            this.durableDataLogFactory = null;
        }

        if (this.underlyingStorageFactory != null) {
            this.underlyingStorageFactory.close();
            this.underlyingStorageFactory = null;
        }
    }

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig.Builder builderConfig, int instanceId) {
        return ServiceBuilder.newInMemoryBuilder(builderConfig.build())
                             .withStorageFactory(setup -> this.storageFactory)
                             .withDataLogFactory(setup -> this.durableDataLogFactory);
    }

    @Override
    @Ignore
    public void testEndToEndWithFencing() {
    }

    private static class PermanentDurableDataLogFactory extends InMemoryDurableDataLogFactory {
        PermanentDurableDataLogFactory(ScheduledExecutorService executorService) {
            super(executorService);
        }

        @Override
        public void close() {
            // This method intentionally left blank; we want this factory to live between multiple recovery attempts.
        }
    }
}
