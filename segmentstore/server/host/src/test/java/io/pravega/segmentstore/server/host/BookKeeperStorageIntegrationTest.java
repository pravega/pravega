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
import io.pravega.segmentstore.server.store.StreamSegmentStoreTestBase;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.bookkeeperstorage.BookKeeperStorageConfig;
import io.pravega.segmentstore.storage.impl.bookkeeperstorage.BookKeeperStorageFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;

/**
 * End-to-end tests for SegmentStore, with integrated Storage and DurableDataLog.
 */
public class BookKeeperStorageIntegrationTest extends StreamSegmentStoreTestBase {
    //region Test Configuration and Setup

    private static final int BOOKIE_COUNT = 1;
    private BookKeeperRunner bookkeeper = null;

    /**
     * Starts BookKeeper.
     */
    @Before
    public void setUp() throws Exception {
        bookkeeper = new BookKeeperRunner(this.configBuilder, BOOKIE_COUNT);
        bookkeeper.initialize();

        this.configBuilder.include(BookKeeperStorageConfig
                .builder()
                .with(BookKeeperStorageConfig.ZK_ADDRESS, "localhost:" + bookkeeper.getZkPort())
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
        bookkeeper.close();
    }

    //endregion

    //region StreamSegmentStoreTestBase Implementation

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig builderConfig) {
        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withCacheFactory(setup -> new RocksDBCacheFactory(builderConfig.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> {
                    StorageFactory f = new LocalBookKeeperStorageFactory(setup.getConfig(BookKeeperStorageConfig::builder), bookkeeper.getZkClient(), setup.getExecutor());
                    return new ListenableStorageFactory(f);
                })
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder), bookkeeper.getZkClient(), setup.getExecutor()));
    }

    private class LocalBookKeeperStorageFactory extends BookKeeperStorageFactory {

        private Storage storage = null;

        public LocalBookKeeperStorageFactory(BookKeeperStorageConfig config, CuratorFramework zkClient, ScheduledExecutorService executor) {
            super(config, zkClient, executor);
        }

        @Override
        public synchronized Storage createStorageAdapter() {
            if (storage == null) {
                storage = super.createStorageAdapter();
            }
            return storage;
        }
    }

    //endregion
}
