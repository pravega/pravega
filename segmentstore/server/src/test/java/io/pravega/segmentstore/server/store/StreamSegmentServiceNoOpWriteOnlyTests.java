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
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Unit tests for the StreamSegmentService using NoOpStorage.
 * Note this end-to-end test does not include the verification of segment content,
 * because user segment is not written to storage.
 */
@Slf4j
public class StreamSegmentServiceNoOpWriteOnlyTests extends StreamSegmentStoreTestBase {

    private NoOpStorageFactory storageFactory;
    private InMemoryDurableDataLogFactory durableDataLogFactory;
    private InMemoryStorageFactory systemStorageFactory;
    // userStorageFactory is null, all user segment write operations are no-oped.
    private final InMemoryStorageFactory userStorageFactory = null;

    @Before
    public void setUp() {
        this.systemStorageFactory = new InMemoryStorageFactory(executorService());
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
    }

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig.Builder builderConfig, int instanceId) {
        return ServiceBuilder.newInMemoryBuilder(builderConfig.build())
                .withStorageFactory(setup -> this.storageFactory)
                .withDataLogFactory(setup -> this.durableDataLogFactory);
    }

    /**
     * Ignore testEndToEnd because it includes segment content verification,
     * which is not possible when write is no-oped.
     */
    @Override
    @Ignore
    public void testEndToEnd() throws Exception {
    }

    /**
     * Ignore testEndToEndWithFenching because it includes segment content verification,
     * which is not possible when write is no-oped.
     */
    @Override
    @Ignore
    public void testEndToEndWithFencing() throws Exception {
    }

    /**
     * Trigger the endToEndProcess without verification of segment content.
     *
     * @throws Exception
     */
    @Test
    public void testEndToEndWriteOnly() throws Exception {
        endToEndProcess(false);
    }

    /**
     * Trigger the endToEndProcessWithFencing without verification of segment content.
     *
     * @throws Exception
     */
    @Test
    public void testEndToEndWithFenchingWriteOnly() throws Exception {
        endToEndProcessWithFencing(false);
    }

}
