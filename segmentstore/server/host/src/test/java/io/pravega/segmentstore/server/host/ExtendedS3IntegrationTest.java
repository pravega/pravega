/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import com.emc.object.s3.S3Config;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.google.common.base.Preconditions;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.StreamSegmentStoreTestBase;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.extendeds3.ExtendedS3Storage;
import io.pravega.segmentstore.storage.impl.extendeds3.ExtendedS3StorageConfig;
import io.pravega.segmentstore.storage.impl.extendeds3.S3FileSystemImpl;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.test.common.TestUtils;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import org.junit.After;
import org.junit.Before;

/**
 * End-to-end tests for SegmentStore, with integrated Extended S3 Storage and DurableDataLog.
 */
public class ExtendedS3IntegrationTest extends StreamSegmentStoreTestBase {
    //region Test Configuration and Setup

    private static final int BOOKIE_COUNT = 1;
    private String endpoint;
    private BookKeeperRunner bookkeeper = null;
    private File baseDir = null;
    private File rocksDBDir = null;
    private S3FileSystemImpl filesystemS3;

    /**
     * Starts BookKeeper.
     */
    @Before
    public void setUp() throws Exception {
        bookkeeper = new BookKeeperRunner(this.configBuilder, BOOKIE_COUNT);
        bookkeeper.initialize();
        endpoint = "http://127.0.0.1:" + TestUtils.getAvailableListenPort();
        URI uri = URI.create(endpoint);
        baseDir = Files.createTempDirectory("extendeds3_wrapper").toFile().getAbsoluteFile();
        rocksDBDir = Files.createTempDirectory("rocksdb").toFile().getAbsoluteFile();
        filesystemS3 = new S3FileSystemImpl(baseDir.toString());
        this.configBuilder.include(ExtendedS3StorageConfig.builder()
                                                          .with(ExtendedS3StorageConfig.BUCKET, "kanpravegatest")
                                                          .with(ExtendedS3StorageConfig.ACCESS_KEY_ID, "x")
                                                          .with(ExtendedS3StorageConfig.SECRET_KEY, "x")
                                                          .with(ExtendedS3StorageConfig.URI, endpoint))
                          .include(RocksDBConfig.builder()
                                                .with(RocksDBConfig.DATABASE_DIR, rocksDBDir.toString()));
    }

    /**
     * Shuts down BookKeeper and cleans up file system directory
     */
    @After
    public void tearDown() throws Exception {
        bookkeeper.close();
        if (baseDir != null) {
            FileHelpers.deleteFileOrDirectory(baseDir);
        }
        if (rocksDBDir != null) {
            FileHelpers.deleteFileOrDirectory(rocksDBDir);
        }

        baseDir = null;
        rocksDBDir = null;
    }

    //endregion

    //region StreamSegmentStoreTestBase Implementation

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig builderConfig) {
        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withCacheFactory(setup -> new RocksDBCacheFactory(builderConfig.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> new LocalExtendedS3StorageFactory(setup.getConfig(ExtendedS3StorageConfig::builder)))
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder),
                        bookkeeper.getZkClient(), setup.getCoreExecutor()));
    }


    /**
     * We are declaring a local factory here because we need a factory that creates adapters that interact
     * with the local file system for the purposes of testing. Ideally, however, we should mock the extended
     * S3 service rather than implement the storage functionality directly in the adapter.
     */
    private class LocalExtendedS3StorageFactory implements StorageFactory {

        private final ExtendedS3StorageConfig config;

        LocalExtendedS3StorageFactory(ExtendedS3StorageConfig config) {
            Preconditions.checkNotNull(config, "config");
            this.config = config;
        }

        @Override
        public Storage createStorageAdapter() {
            URI uri = URI.create(endpoint);
            S3Config s3Config = new S3Config(uri);

            s3Config = s3Config.withIdentity(config.getAccessKey()).withSecretKey(config.getSecretKey())
                    .withRetryEnabled(false)
                    .withInitialRetryDelay(1)
                    .withProperty("com.sun.jersey.client.property.connectTimeout", 100);

            S3JerseyClient client = new S3ClientWrapper(s3Config, filesystemS3);
            return new AsyncStorageWrapper(new RollingStorage(new ExtendedS3Storage(client, config)), executorService());
        }
    }
    //endregion
}
