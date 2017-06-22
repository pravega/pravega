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

import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.StreamSegmentStoreTestBase;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.filesystem.FileSystemStorageConfig;
import io.pravega.segmentstore.storage.impl.filesystem.FileSystemStorageFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import java.io.File;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;

/**
 * End-to-end tests for SegmentStore, with integrated Storage and DurableDataLog.
 */
public class SegmentStoreFileSystemIntegrationTest extends StreamSegmentStoreTestBase {
    //region Test Configuration and Setup

    private static final int BOOKIE_COUNT = 3;
    private File baseDir = null;
    private BookKeeperRunner helper = null;
    /**
     * Starts BookKeeper.
     */
    @Before
    public void setUp() throws Exception {
        helper = new BookKeeperRunner(this.configBuilder, BOOKIE_COUNT);
        helper.initialize();

        this.baseDir = Files.createTempDirectory("test_fs").toFile().getAbsoluteFile();

        this.configBuilder.include(FileSystemStorageConfig
                .builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath()));
    }

    /**
     * Shuts down BookKeeper and cleans up file system directory
     */
    @After
    public void tearDown() throws Exception {
        helper.close();
        FileHelpers.deleteFileOrDirectory(this.baseDir);
        this.baseDir = null;
    }

    //endregion

    //region StreamSegmentStoreTestBase Implementation

    @Override
    protected synchronized ServiceBuilder createBuilder(ServiceBuilderConfig builderConfig,
                                                        AtomicReference<Storage> storage) {
        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withCacheFactory(setup -> new RocksDBCacheFactory(builderConfig.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> {
                    StorageFactory f = new FileSystemStorageFactory(
                            setup.getConfig(FileSystemStorageConfig::builder), setup.getExecutor());
                    return new ListenableStorageFactory(f, storage::set);
                })
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder),
                                                            helper.getZkClient(), setup.getExecutor()));
    }

    //endregion
}
