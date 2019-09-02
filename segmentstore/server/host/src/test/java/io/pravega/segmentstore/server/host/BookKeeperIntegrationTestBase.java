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

import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.server.store.StreamSegmentStoreTestBase;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import lombok.AccessLevel;
import lombok.Getter;

/**
 * Base class for any StreamSegmentStore Integration Test that uses BookKeeper and RocksDB.
 */
abstract class BookKeeperIntegrationTestBase extends StreamSegmentStoreTestBase {
    private static final int BOOKIE_COUNT = 1;
    @Getter(AccessLevel.PROTECTED)
    private File baseDir = null;
    @Getter(AccessLevel.PROTECTED)
    private File rocksDBDir = null;
    @Getter(AccessLevel.PROTECTED)
    private BookKeeperRunner bookkeeper = null;

    /**
     * Starts BookKeeper and sets up RocksDB.
     *
     * NOTE: this (and tearDown()) cannot be annotated with @Before and @After since JUnit doesn't pick these up from
     * super classes, at least not in the order in which we expect it to)
     */
    protected void setUp() throws Exception {
        bookkeeper = new BookKeeperRunner(this.configBuilder, BOOKIE_COUNT);
        bookkeeper.initialize();

        this.baseDir = Files.createTempDirectory("IntegrationTest").toFile().getAbsoluteFile();
        this.rocksDBDir = Files.createTempDirectory("rocksdb").toFile().getAbsoluteFile();
    }

    /**
     * Shuts down BookKeeper and RocksDB and cleans up file system directory.
     */
    protected void tearDown() throws Exception {
        bookkeeper.close();
        if (baseDir != null) {
            FileHelpers.deleteFileOrDirectory(this.baseDir);
        }

        if (baseDir != null) {
            FileHelpers.deleteFileOrDirectory(this.rocksDBDir);
        }

        this.baseDir = null;
        this.rocksDBDir = null;
    }

    /**
     * Creates a ServiceBuilderConfig based on the given builder, by attaching the correct RocksDB file path based on the instance.
     *
     * @param configBuilder The ServiceBuilderConfig.Builder to base from (this builder will not be touched).
     * @param instanceId    The instance id of the Service to build (for least interference, different instances should have
     *                      different Ids so that shared resources (i.e., RocksDB) can be setup appropriately).
     * @return A ServiceBuilderConfig instance.
     */
    protected ServiceBuilderConfig getBuilderConfig(ServiceBuilderConfig.Builder configBuilder, int instanceId) {
        String id = Integer.toString(instanceId);
        return configBuilder
                .makeCopy()
                .include(ServiceConfig.builder().with(ServiceConfig.INSTANCE_ID, id))
                .include(RocksDBConfig.builder().with(RocksDBConfig.DATABASE_DIR, Paths.get(getRocksDBDir().toString(), id).toString()))
                .build();
    }

    @Override
    protected double getFencingTestOperationMultiplier() {
        return 0.3; // Adding operations one-by-one using BookKeeper is much slower than bulk-adding them.
    }
}
