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
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.StreamSegmentStoreTestBase;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.segmentstore.storage.impl.extendeds3.AclSize;
import io.pravega.segmentstore.storage.impl.extendeds3.ExtendedS3Storage;
import io.pravega.segmentstore.storage.impl.extendeds3.ExtendedS3StorageConfig;
import io.pravega.segmentstore.storage.impl.extendeds3.ExtendedS3StorageFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBCacheFactory;
import io.pravega.segmentstore.storage.impl.rocksdb.RocksDBConfig;
import io.pravega.test.common.TestUtils;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.junit.After;
import org.junit.Before;

/**
 * End-to-end tests for SegmentStore, with integrated Extended S3 Storage and DurableDataLog.
 */
public class ExtendedS3IntegrationTest extends StreamSegmentStoreTestBase {
    //region Test Configuration and Setup

    private static final int BOOKIE_COUNT = 3;
    private BookKeeperRunner bookkeeper = null;
    private String baseDir;
    private ConcurrentMap<String, AclSize> aclMap = new ConcurrentHashMap<>();

    /**
     * Starts BookKeeper.
     */
    @Before
    public void setUp() throws Exception {
        bookkeeper = new BookKeeperRunner(this.configBuilder, BOOKIE_COUNT);
        bookkeeper.initialize();

        String endpoint = "http://127.0.0.1:" + TestUtils.getAvailableListenPort();

        URI uri = URI.create(endpoint);

        Properties properties = new Properties();
        properties.setProperty("s3proxy.authorization", "none");
        properties.setProperty("s3proxy.endpoint", endpoint);
        properties.setProperty("jclouds.provider", "filesystem");
        properties.setProperty("jclouds.filesystem.basedir", "/tmp/s3proxy");
        baseDir = Files.createTempDirectory("extendeds3_wrapper").toString();

        this.configBuilder.include(ExtendedS3StorageConfig.builder()
                                                          .with(ExtendedS3StorageConfig.BUCKET, "kanpravegatest")
                                                          .with(ExtendedS3StorageConfig.ACCESS_KEY_ID, "x")
                                                          .with(ExtendedS3StorageConfig.SECRET_KEY, "x")
                                                          .with(ExtendedS3StorageConfig.URI, "http://localhost:9020"));
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
                    StorageFactory f = new ExtendedS3StorageFactory(
                            setup.getConfig(ExtendedS3StorageConfig::builder), setup.getExecutor());
                    return new ExtendedS3ListenableStorageFactory(f, builderConfig.getConfig(ExtendedS3StorageConfig::builder), this.aclMap);
                })
                .withDataLogFactory(setup -> new BookKeeperLogFactory(setup.getConfig(BookKeeperConfig::builder),
                        bookkeeper.getZkClient(), setup.getExecutor()));
    }


    private class ExtendedS3ListenableStorageFactory extends ListenableStorageFactory {

        private final ExtendedS3StorageConfig adapterConfig;
        private final ConcurrentMap<String, AclSize> aclMap;

        public ExtendedS3ListenableStorageFactory(StorageFactory wrappedFactory, ExtendedS3StorageConfig adapterConfig, ConcurrentMap<String, AclSize> aclMap) {
            super(wrappedFactory);
            this.adapterConfig = adapterConfig;
            this.aclMap = aclMap;
        }

        @Override
        public Storage createStorageAdapter() {
            URI uri = URI.create("http://localhost:9020");
            S3Config s3Config = new S3Config(uri);

            s3Config = s3Config.withIdentity(adapterConfig.getAccessKey()).withSecretKey(adapterConfig.getSecretKey())
                               .withRetryEnabled(false).withInitialRetryDelay(1).withProperty("com.sun.jersey.client.property.connectTimeout", 100);

            S3JerseyClient client = new S3ClientWrapper(s3Config, this.aclMap, baseDir);
            storage = new ExtendedS3Storage(client, adapterConfig, executorService());
            return storage;
        }
    }

    //endregion
}
