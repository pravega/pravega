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
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.storage.filesystem.FileSystemSimpleStorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.filesystem.FileSystemStorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageFactoryCreator;
import io.pravega.test.common.AssertExtensions;
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
    public void testFileSystemStorageFactoryCreator() {
        StorageFactoryCreator factoryCreator = new FileSystemStorageFactoryCreator();

        val expected = new StorageFactoryInfo[]{
                StorageFactoryInfo.builder()
                        .name("FILESYSTEM")
                        .storageLayoutType(StorageLayoutType.CHUNKED_STORAGE)
                        .build(),
                StorageFactoryInfo.builder()
                        .name("FILESYSTEM")
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

    @Test
    public void testNull() {
        val executor = new ScheduledThreadPoolExecutor(1);
        AssertExtensions.assertThrows(
                " should throw exception.",
                () -> new FileSystemSimpleStorageFactory(null, executor),
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows(
                " should throw exception.",
                () -> new FileSystemSimpleStorageFactory(FileSystemStorageConfig.builder().build(), null),
                ex -> ex instanceof NullPointerException);
    }
}
