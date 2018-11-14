/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.server.host;

import com.emc.object.s3.S3Config;
import com.emc.object.s3.jersey.S3JerseyClient;
import com.google.common.base.Preconditions;
import io.pravega.segmentstore.server.host.BookKeeperIntegrationTestBase;
import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.segmentstore.server.host.S3ClientWrapper;
import io.pravega.storage.extendeds3.ExtendedS3Storage;
import io.pravega.storage.extendeds3.ExtendedS3StorageConfig;
import io.pravega.storage.extendeds3.S3FileSystemImpl;
import io.pravega.test.common.TestUtils;
import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.After;
import org.junit.Before;

/**
 * End-to-end tests for SegmentStore, with integrated Extended S3 Storage and DurableDataLog.
 */
public class ExtendedS3IntegrationTest extends BookKeeperIntegrationTestBase {
    //region Test Configuration and Setup

    private String endpoint;
    private S3FileSystemImpl filesystemS3;

    /**
     * Starts BookKeeper.
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
        endpoint = "http://127.0.0.1:" + TestUtils.getAvailableListenPort();
        URI uri = URI.create(endpoint);
        filesystemS3 = new S3FileSystemImpl(getBaseDir().toString());
        this.configBuilder.include(ExtendedS3StorageConfig.builder()
                                                          .with(ExtendedS3StorageConfig.BUCKET, "kanpravegatest")
                                                          .with(ExtendedS3StorageConfig.ACCESS_KEY_ID, "x")
                                                          .with(ExtendedS3StorageConfig.SECRET_KEY, "x")
                .with(ExtendedS3StorageConfig.URI, endpoint));
    }

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
                .withStorageFactory(setup -> new LocalExtendedS3StorageFactory(setup.getConfig(ExtendedS3StorageConfig::builder), setup.getStorageExecutor()))
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder),
                        getBookkeeper().getZkClient(), setup.getCoreExecutor()));
    }


    /**
     * We are declaring a local factory here because we need a factory that creates adapters that interact
     * with the local file system for the purposes of testing. Ideally, however, we should mock the extended
     * S3 service rather than implement the storage functionality directly in the adapter.
     */
    private class LocalExtendedS3StorageFactoryCreator implements StorageFactoryCreator {

        @Override
        public StorageFactory createFactory(ConfigSetup setup, ScheduledExecutorService executor) {
            return new LocalExtendedS3StorageFactory(setup.getConfig(ExtendedS3StorageConfig::builder), executor);
        }

        @Override
        public String getName() {
            return "LocalExtendedStorageFactory";
        }
    }

    private class LocalExtendedS3StorageFactory implements StorageFactory {

        private final ExtendedS3StorageConfig config;
        private final ScheduledExecutorService storageExecutor;

        LocalExtendedS3StorageFactory(ExtendedS3StorageConfig config, ScheduledExecutorService executor) {
            this.config = Preconditions.checkNotNull(config, "config");
            this.storageExecutor = Preconditions.checkNotNull(executor, "executor");
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
            return new AsyncStorageWrapper(new RollingStorage(new ExtendedS3Storage(client, config)), this.storageExecutor);
        }
    }
    //endregion
}
