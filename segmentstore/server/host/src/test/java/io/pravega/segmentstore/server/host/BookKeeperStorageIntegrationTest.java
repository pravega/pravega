/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeperstorage.BookKeeperStorageConfig;
import io.pravega.segmentstore.storage.impl.bookkeeperstorage.BookKeeperStorageFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import org.junit.After;
import org.junit.Before;

/**
 * End-to-end tests for SegmentStore, with integrated Storage and DurableDataLog.
 */
public class BookKeeperStorageIntegrationTest extends BookKeeperIntegrationTestBase {

    /**
     * Starts BookKeeper.
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();

        this.configBuilder.include(BookKeeperStorageConfig
                .builder()
                .with(BookKeeperStorageConfig.ZK_ADDRESS, bookkeeper.getZkClient().getZookeeperClient().getCurrentConnectionString())
                .with(BookKeeperStorageConfig.BK_ACK_QUORUM_SIZE, BOOKIE_COUNT)
                .with(BookKeeperStorageConfig.BK_LEDGER_PATH, "/ledgers")
                .with(BookKeeperStorageConfig.BK_ENSEMBLE_SIZE, BOOKIE_COUNT)
                .with(BookKeeperStorageConfig.BK_WRITE_QUORUM_SIZE, BOOKIE_COUNT)
        );
    }

    /**
     * Shuts down BookKeeper.
     */
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    //endregion

    //region StreamSegmentStoreTestBase Implementation
    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig.Builder configBuilder, int instanceId) {
        ServiceBuilderConfig builderConfig = getBuilderConfig(configBuilder, instanceId);
        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withCacheFactory(setup -> new RocksDBCacheFactory(builderConfig.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> new BookKeeperStorageFactory(setup.getConfig(BookKeeperStorageConfig::builder), bookkeeper.getZkClient(), setup.getStorageExecutor()))
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder), bookkeeper.getZkClient(), setup.getCoreExecutor()));

    }

    //endregion
}