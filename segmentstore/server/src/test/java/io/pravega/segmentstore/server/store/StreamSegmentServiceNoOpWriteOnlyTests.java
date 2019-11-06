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
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the StreamSegmentService using NoOpStorage.
 * Note this end-to-end test does not include the verification of segment content,
 * because user segment write operation is no-oped.
 */
@Slf4j
public class StreamSegmentServiceNoOpWriteOnlyTests extends StreamSegmentStoreTestBase {

    private NoOpStorageFactory storageFactory;
    private InMemoryDurableDataLogFactory durableDataLogFactory;
    private InMemoryStorageFactory systemStorageFactory;

    @Before
    public void setUp() {
        this.systemStorageFactory = new InMemoryStorageFactory(executorService());
        StorageExtraConfig config = StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_NO_OP_MODE, true)
                .with(StorageExtraConfig.STORAGE_WRITE_NO_OP_LATENCY, 100)
                .build();
        //Note userStorageFactory is null, then all user segment write operations are no-oped
        this.storageFactory = new NoOpStorageFactory(config, executorService(), systemStorageFactory, null);
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
    }

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig.Builder builderConfig, int instanceId) {
        return ServiceBuilder.newInMemoryBuilder(builderConfig.build())
                .withStorageFactory(setup -> this.storageFactory)
                .withDataLogFactory(setup -> this.durableDataLogFactory);
    }

    /**
     * Trigger the endToEndProcess without the verification of segment content.
     * It is not possible to verify segment content when write is no-oped.
     */
    @Override
    @Test
    public void testEndToEnd() throws Exception {
        endToEndProcess(false);
    }

    /**
     * Trigger the endToEndProcessWithFencing without the verification of segment content.
     * It is not possible to verify segment content when write is no-oped.
     */
    @Override
    @Test
    public void testEndToEndWithFencing() throws Exception {
        endToEndProcessWithFencing(false);
    }
}
