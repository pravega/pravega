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

import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.segmentstore.storage.noop.NoOpStorageFactory;
import io.pravega.segmentstore.storage.noop.StorageExtraConfig;
import io.pravega.storage.extendeds3.ExtendedS3SimpleStorageFactory;
import io.pravega.storage.extendeds3.ExtendedS3StorageConfig;
import io.pravega.storage.extendeds3.ExtendedS3StorageFactory;
import io.pravega.storage.filesystem.FileSystemSimpleStorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import io.pravega.storage.hdfs.HDFSSimpleStorageFactory;
import io.pravega.storage.hdfs.HDFSStorageFactory;
import lombok.val;
import org.junit.Test;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.junit.Assert.assertTrue;

public class StorageLoaderTest {

    private StorageFactory expectedFactory;

    @Test
    public void testNoOpWithWithInMemoryStorage() throws Exception {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                .builder()
                .include(StorageExtraConfig.builder()
                        .with(StorageExtraConfig.STORAGE_NO_OP_MODE, true))
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1)
                        .with(ServiceConfig.STORAGE_IMPLEMENTATION, ServiceConfig.StorageType.INMEMORY));

        ServiceBuilder builder = ServiceBuilder.newInMemoryBuilder(configBuilder.build())
                .withStorageFactory(setup -> {
                    StorageLoader loader = new StorageLoader();
                    expectedFactory = loader.load(setup, "INMEMORY", false, true,  executor);
                    return expectedFactory;
                });
        builder.initialize();
        assertTrue(expectedFactory instanceof NoOpStorageFactory);
        builder.close();

        configBuilder
                .include(StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_NO_OP_MODE, false));

        builder = ServiceBuilder.newInMemoryBuilder(configBuilder.build())
                .withStorageFactory(setup -> {
                    StorageLoader loader = new StorageLoader();
                    expectedFactory = loader.load(setup, "INMEMORY", false, true, executor);
                    return expectedFactory;
                });
        builder.initialize();
        assertTrue(expectedFactory instanceof InMemoryStorageFactory);
        builder.close();
    }

    @Test
    public void testFileSystemStorage() throws Exception {
        val storageType = ServiceConfig.StorageType.FILESYSTEM;
        boolean isChunkManagerSupported = false;
        boolean isLegacyLayout = true;
        ServiceBuilder builder = getStorageFactory(storageType, "FILESYSTEM", isChunkManagerSupported, isLegacyLayout);
        assertTrue(expectedFactory instanceof FileSystemStorageFactory);
        builder.close();
    }

    @Test
    public void testSimpleFileSystemStorage() throws Exception {
        val storageType = ServiceConfig.StorageType.FILESYSTEM;
        boolean isChunkManagerSupported = true;
        boolean isLegacyLayout = false;
        ServiceBuilder builder = getStorageFactory(storageType, "FILESYSTEM", isChunkManagerSupported, isLegacyLayout);
        assertTrue(expectedFactory instanceof FileSystemSimpleStorageFactory);
        builder.close();
    }


    @Test
    public void testHDFSStorage() throws Exception {
        val storageType = ServiceConfig.StorageType.HDFS;
        boolean isChunkManagerSupported = false;
        boolean isLegacyLayout = true;
        ServiceBuilder builder = getStorageFactory(storageType, "HDFS", isChunkManagerSupported, isLegacyLayout);
        assertTrue(expectedFactory instanceof HDFSStorageFactory);
        builder.close();
    }

    @Test
    public void testHDFSSimpleStorage() throws Exception {
        val storageType = ServiceConfig.StorageType.HDFS;
        boolean isChunkManagerSupported = true;
        boolean isLegacyLayout = false;
        ServiceBuilder builder = getStorageFactory(storageType, "HDFS", isChunkManagerSupported, isLegacyLayout);
        assertTrue(expectedFactory instanceof HDFSSimpleStorageFactory);
        builder.close();
    }

    @Test
    public void testExtendedS3Storage() throws Exception {
        val storageType = ServiceConfig.StorageType.EXTENDEDS3;
        boolean isChunkManagerSupported = false;
        boolean isLegacyLayout = true;
        ServiceBuilder builder = getStorageFactory(storageType, "EXTENDEDS3",  isChunkManagerSupported, isLegacyLayout);
        assertTrue(expectedFactory instanceof ExtendedS3StorageFactory);
        builder.close();
    }

    @Test
    public void testExtendedS3SimpleStorage() throws Exception {
        val storageType = ServiceConfig.StorageType.EXTENDEDS3;
        boolean isChunkManagerSupported = true;
        boolean isLegacyLayout = false;
        ServiceBuilder builder = getStorageFactory(storageType, "EXTENDEDS3", isChunkManagerSupported, isLegacyLayout);
        assertTrue(expectedFactory instanceof ExtendedS3SimpleStorageFactory);
        builder.close();
    }

    private ServiceBuilder getStorageFactory(ServiceConfig.StorageType storageType, String name, boolean isChunkManagerSupported, boolean isLegacyLayout) throws DurableDataLogException {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                .builder()
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1)
                        .with(ServiceConfig.STORAGE_IMPLEMENTATION, storageType))
                .include(ExtendedS3StorageConfig.builder()
                                .with(ExtendedS3StorageConfig.CONFIGURI, "http://127.0.0.1?identity=x&secretKey=x")
                                .with(ExtendedS3StorageConfig.BUCKET, "bucket"));

        ServiceBuilder builder = ServiceBuilder.newInMemoryBuilder(configBuilder.build())
                .withStorageFactory(setup -> {
                    StorageLoader loader = new StorageLoader();
                    expectedFactory = loader.load(setup, name, isChunkManagerSupported, isLegacyLayout,  executor);
                    return expectedFactory;
                });
        builder.initialize();
        return builder;
    }
}