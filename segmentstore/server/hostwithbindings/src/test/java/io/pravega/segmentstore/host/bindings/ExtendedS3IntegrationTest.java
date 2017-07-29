/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.host.bindings;

import com.emc.object.s3.S3Config;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.google.common.base.Preconditions;
import io.pravega.common.io.FileHelpers;
import io.pravega.common.util.ServiceBuilderConfig;
import io.pravega.segmentstore.server.host.BookKeeperRunner;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.StreamSegmentStoreTestBase;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.bindings.extendeds3.ExtendedS3Storage;
import io.pravega.segmentstore.storage.bindings.extendeds3.ExtendedS3StorageConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import io.pravega.test.common.TestUtils;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import org.junit.After;
import org.junit.Before;

/**
 * End-to-end tests for SegmentStore, with integrated Extended S3 Storage and DurableDataLog.
 */
public class ExtendedS3IntegrationTest extends StreamSegmentStoreTestBase {
    //region Test Configuration and Setup

    private static final int BOOKIE_COUNT = 3;
    private String endpoint;
    private BookKeeperRunner bookkeeper = null;
    private String baseDir;
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
        baseDir = Files.createTempDirectory("extendeds3_wrapper").toString();
        filesystemS3 = new S3FileSystemImpl(baseDir);
        this.configBuilder.include(ExtendedS3StorageConfig.builder()
                                                          .with(ExtendedS3StorageConfig.BUCKET, "kanpravegatest")
                                                          .with(ExtendedS3StorageConfig.ACCESS_KEY_ID, "x")
                                                          .with(ExtendedS3StorageConfig.SECRET_KEY, "x")
                                                          .with(ExtendedS3StorageConfig.URI, endpoint));
    }

    /**
     * Shuts down BookKeeper and cleans up file system directory
     */
    @After
    public void tearDown() throws Exception {
        bookkeeper.close();
        FileHelpers.deleteFileOrDirectory(new File(baseDir));
    }

    //endregion

    //region StreamSegmentStoreTestBase Implementation

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig builderConfig) {
        return ServiceBuilder
                .newInMemoryBuilder(builderConfig)
                .withCacheFactory(setup -> new RocksDBCacheFactory(builderConfig.getConfig(RocksDBConfig::builder)))
                .withStorageFactory(setup -> {
                    StorageFactory f = new LocalExtendedS3StorageFactory(
                            setup.getConfig(ExtendedS3StorageConfig::builder),
                            setup.getExecutor());
                    return new ListenableStorageFactory(f);
                })
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder),
                        bookkeeper.getZkClient(), setup.getExecutor()));
    }


    /*
     * We are declaring a local factory here because we need a factory that creates adapters that interact
     * with the local file system for the purposes of testing. Ideally, however, we should mock the extended
     * S3 service rather than implement the storage functionality directly in the adapter.
     */
    private class LocalExtendedS3StorageFactory implements StorageFactory {

        private final ExtendedS3StorageConfig config;
        private final ExecutorService executor;

        public LocalExtendedS3StorageFactory(ExtendedS3StorageConfig config,
                                             ExecutorService executor) {
            Preconditions.checkNotNull(config, "config");
            Preconditions.checkNotNull(executor, "executor");
            this.config = config;
            this.executor = executor;
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
            return new ExtendedS3Storage(client, config, executorService());
        }
    }
    //endregion
}
