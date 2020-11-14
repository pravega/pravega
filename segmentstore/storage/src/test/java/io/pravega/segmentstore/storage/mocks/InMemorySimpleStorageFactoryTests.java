/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import io.pravega.segmentstore.storage.StorageFactoryInfo;
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.any;

/**
 * Unit tests for {@link InMemorySimpleStorageFactory}.
 */
public class InMemorySimpleStorageFactoryTests {
    @Test
    public void testInMemorySimpleStorageFactoryCreator() {
        StorageFactoryCreator factoryCreator = new InMemoryStorageFactoryCreator();

        val expected = new StorageFactoryInfo[]{
                StorageFactoryInfo.builder()
                        .name("INMEMORY")
                        .storageLayoutType(StorageLayoutType.CHUNKED_STORAGE)
                        .build(),
                StorageFactoryInfo.builder()
                        .name("INMEMORY")
                        .storageLayoutType(StorageLayoutType.ROLLING_STORAGE)
                        .build()
        };

        val factoryInfoList = factoryCreator.getStorageFactories();
        Assert.assertEquals(2, factoryInfoList.length);
        Assert.assertArrayEquals(expected, factoryInfoList);

        // Simple Storage
        ConfigSetup configSetup1 = mock(ConfigSetup.class);
        val config = new Object();
        when(configSetup1.getConfig(any())).thenReturn(config);
        val factory1 = factoryCreator.createFactory(expected[0], configSetup1, new ScheduledThreadPoolExecutor(1));
        Assert.assertTrue(factory1 instanceof InMemorySimpleStorageFactory);

        Storage storage1 = factory1.createStorageAdapter();
        Assert.assertTrue(storage1 instanceof ChunkedSegmentStorage);

        // Legacy Storage
        ConfigSetup configSetup2 = mock(ConfigSetup.class);
        when(configSetup2.getConfig(any())).thenReturn(config);
        val factory2 = factoryCreator.createFactory(expected[1], configSetup2, new ScheduledThreadPoolExecutor(1));

        Assert.assertTrue(factory2 instanceof InMemoryStorageFactory);
        Storage storage2 = factory2.createStorageAdapter();
        Assert.assertTrue(storage2 instanceof AsyncStorageWrapper);

        SyncStorage syncStorage = factory2.createSyncStorage();
        Assert.assertNotNull(syncStorage);
    }

    @Test
    public void testReuse() {
        val factory = new InMemorySimpleStorageFactory(new ScheduledThreadPoolExecutor(1), true);

        val s1 = (ChunkedSegmentStorage) factory.createStorageAdapter();
        val s2 = (ChunkedSegmentStorage) factory.createStorageAdapter();

        Assert.assertEquals(s1.getChunkStorage(), s2.getChunkStorage());
    }

    @Test
    public void testNoReuse() {
        val factory = new InMemorySimpleStorageFactory(new ScheduledThreadPoolExecutor(1), false);

        val s1 = (ChunkedSegmentStorage) factory.createStorageAdapter();
        val s2 = (ChunkedSegmentStorage) factory.createStorageAdapter();

        Assert.assertNotEquals(s1.getChunkStorage(), s2.getChunkStorage());
    }
}
