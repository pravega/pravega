/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
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

/**
 * Unit tests for the StreamSegmentService using NoOpStorage.
 * Note this end-to-end test includes the verification of segment content read from storage,
 * userStorageFactory is created to accommodate user segments for the purpose.
 */
public class StreamSegmentServiceNoOpWriteReadTests extends StreamSegmentStoreTestBase {

    private NoOpStorageFactory storageFactory;
    private InMemoryDurableDataLogFactory durableDataLogFactory;

    //The underlying factory to create system storage for system segments.
    private InMemoryStorageFactory systemStorageFactory;
    //The underlying factory to create user storage for user segments; optional. Write operation is no-oped if this factory not present.
    private InMemoryStorageFactory userStorageFactory;

    @Before
    public void setUp() {
        this.systemStorageFactory = new InMemoryStorageFactory(executorService());
        this.userStorageFactory = new InMemoryStorageFactory(executorService());

        StorageExtraConfig config = StorageExtraConfig.builder().with(StorageExtraConfig.STORAGE_NO_OP_MODE, true).build();
        this.storageFactory = new NoOpStorageFactory(config, executorService(), systemStorageFactory, userStorageFactory);
        this.durableDataLogFactory = new StreamSegmentServiceTests.PermanentDurableDataLogFactory(executorService());
    }

    @After
    public void tearDown() {
        if (this.durableDataLogFactory != null) {
            this.durableDataLogFactory.close();
            this.durableDataLogFactory = null;
        }

        if (this.systemStorageFactory != null) {
            this.systemStorageFactory.close();
            this.systemStorageFactory = null;
        }

        if (this.userStorageFactory != null) {
            this.userStorageFactory.close();
            this.userStorageFactory = null;
        }
    }

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig.Builder builderConfig, int instanceId) {
        return ServiceBuilder.newInMemoryBuilder(builderConfig.build())
                             .withStorageFactory(setup -> this.storageFactory)
                             .withDataLogFactory(setup -> this.durableDataLogFactory);
    }
}
