/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage;

import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import io.pravega.segmentstore.storage.StorageFactoryInfo;
import io.pravega.segmentstore.storage.StorageMetadataFormat;
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.storage.extendeds3.ExtendedS3SimpleStorageFactory;
import io.pravega.storage.extendeds3.ExtendedS3StorageConfig;
import io.pravega.storage.extendeds3.ExtendedS3StorageFactory;
import io.pravega.storage.extendeds3.ExtendedS3StorageFactoryCreator;
import io.pravega.storage.filesystem.FileSystemSimpleStorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageFactoryCreator;
import io.pravega.storage.hdfs.HDFSSimpleStorageFactory;
import io.pravega.storage.hdfs.HDFSStorageConfig;
import io.pravega.storage.hdfs.HDFSStorageFactory;
import io.pravega.storage.hdfs.HDFSStorageFactoryCreator;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test factories.
 */
public class StorageFactoryTests {
    @Test
    public void testHDFSStorageFactoryCreator() {
        StorageFactoryCreator factoryCreator = new HDFSStorageFactoryCreator();

        val expected = new StorageFactoryInfo[]{
                StorageFactoryInfo.builder()
                        .name("HDFS")
                        .storageMetadataFormat(StorageMetadataFormat.TABLE_BASED)
                        .storageLayoutType(StorageLayoutType.CHUNKED_STORAGE)
                        .build(),
                StorageFactoryInfo.builder()
                        .name("HDFS")
                        .storageMetadataFormat(StorageMetadataFormat.HEADER_BASED)
                        .storageLayoutType(StorageLayoutType.ROLLING_STORAGE)
                        .build()
        };
        val factoryInfoList = factoryCreator.getStorageFactories();
        Assert.assertEquals(2, factoryInfoList.length);
        Assert.assertArrayEquals(expected, factoryInfoList);

        // Simple Storage
        ConfigSetup configSetup1 = mock(ConfigSetup.class);
        when(configSetup1.getConfig(any())).thenReturn(HDFSStorageConfig.builder().build());
        val factory1 = factoryCreator.createFactory(expected[0], configSetup1, new ScheduledThreadPoolExecutor(1));
        Assert.assertTrue(factory1 instanceof HDFSSimpleStorageFactory);

        Storage storage1 = factory1.createStorageAdapter();
        Assert.assertTrue(storage1 instanceof ChunkedSegmentStorage);

        // Legacy Storage
        ConfigSetup configSetup2 = mock(ConfigSetup.class);
        when(configSetup2.getConfig(any())).thenReturn(HDFSStorageConfig.builder().build());
        val factory2 = factoryCreator.createFactory(expected[1], configSetup2, new ScheduledThreadPoolExecutor(1));
        Assert.assertTrue(factory2 instanceof HDFSStorageFactory);

        Storage storage2 = factory2.createStorageAdapter();
        Assert.assertTrue(storage2 instanceof AsyncStorageWrapper);

        SyncStorage syncStorage = factory2.createSyncStorage();
        Assert.assertNotNull(syncStorage);
    }

    @Test
    public void testExtendedS3StorageFactoryCreator() {
        StorageFactoryCreator factoryCreator = new ExtendedS3StorageFactoryCreator();

        val expected = new StorageFactoryInfo[]{
                StorageFactoryInfo.builder()
                        .name("EXTENDEDS3")
                        .storageMetadataFormat(StorageMetadataFormat.TABLE_BASED)
                        .storageLayoutType(StorageLayoutType.CHUNKED_STORAGE)
                        .build(),
                StorageFactoryInfo.builder()
                        .name("EXTENDEDS3")
                        .storageMetadataFormat(StorageMetadataFormat.HEADER_BASED)
                        .storageLayoutType(StorageLayoutType.ROLLING_STORAGE)
                        .build()
        };

        val factoryInfoList = factoryCreator.getStorageFactories();
        Assert.assertEquals(2, factoryInfoList.length);
        Assert.assertArrayEquals(expected, factoryInfoList);

        // Simple Storage
        ConfigSetup configSetup1 = mock(ConfigSetup.class);
        val config = ExtendedS3StorageConfig.builder()
                .with(ExtendedS3StorageConfig.CONFIGURI, "http://127.0.0.1?identity=x&secretKey=x")
                .with(ExtendedS3StorageConfig.BUCKET, "bucket")
                .with(ExtendedS3StorageConfig.PREFIX, "samplePrefix")
                .build();
        when(configSetup1.getConfig(any())).thenReturn(config);
        val factory1 = factoryCreator.createFactory(expected[0], configSetup1, new ScheduledThreadPoolExecutor(1));
        Assert.assertTrue(factory1 instanceof ExtendedS3SimpleStorageFactory);

        Storage storage1 = factory1.createStorageAdapter();
        Assert.assertTrue(storage1 instanceof ChunkedSegmentStorage);

        // Legacy Storage
        ConfigSetup configSetup2 = mock(ConfigSetup.class);
        when(configSetup2.getConfig(any())).thenReturn(config);
        val factory2 = factoryCreator.createFactory(expected[1], configSetup2, new ScheduledThreadPoolExecutor(1));

        Assert.assertTrue(factory2 instanceof ExtendedS3StorageFactory);
        Storage storage2 = factory2.createStorageAdapter();
        Assert.assertTrue(storage2 instanceof AsyncStorageWrapper);

        SyncStorage syncStorage = factory2.createSyncStorage();
        Assert.assertNotNull(syncStorage);
    }

    @Test
    public void testFileSystemStorageFactoryCreator() {
        StorageFactoryCreator factoryCreator = new FileSystemStorageFactoryCreator();

        val expected = new StorageFactoryInfo[]{
                StorageFactoryInfo.builder()
                        .name("FILESYSTEM")
                        .storageMetadataFormat(StorageMetadataFormat.TABLE_BASED)
                        .storageLayoutType(StorageLayoutType.CHUNKED_STORAGE)
                        .build(),
                StorageFactoryInfo.builder()
                        .name("FILESYSTEM")
                        .storageMetadataFormat(StorageMetadataFormat.HEADER_BASED)
                        .storageLayoutType(StorageLayoutType.ROLLING_STORAGE)
                        .build()
        };

        val factoryInfoList = factoryCreator.getStorageFactories();
        Assert.assertEquals(2, factoryInfoList.length);
        Assert.assertArrayEquals(expected, factoryInfoList);

        // Simple Storage
        ConfigSetup configSetup1 = mock(ConfigSetup.class);
        when(configSetup1.getConfig(any())).thenReturn(FileSystemStorageConfig.builder().build());
        val factory1 = factoryCreator.createFactory(expected[0], configSetup1, new ScheduledThreadPoolExecutor(1));
        Assert.assertTrue(factory1 instanceof FileSystemSimpleStorageFactory);

        Storage storage1 = factory1.createStorageAdapter();
        Assert.assertTrue(storage1 instanceof ChunkedSegmentStorage);

        // Legacy Storage
        ConfigSetup configSetup2 = mock(ConfigSetup.class);
        when(configSetup2.getConfig(any())).thenReturn(FileSystemStorageConfig.builder().build());
        val factory2 = factoryCreator.createFactory(expected[1], configSetup2, new ScheduledThreadPoolExecutor(1));

        Assert.assertTrue(factory2 instanceof FileSystemStorageFactory);
        Storage storage2 = factory2.createStorageAdapter();
        Assert.assertTrue(storage2 instanceof AsyncStorageWrapper);

        SyncStorage syncStorage = factory2.createSyncStorage();
        Assert.assertNotNull(syncStorage);
    }
}
