/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.mocks;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import io.pravega.segmentstore.storage.StorageFactoryInfo;
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link InMemorySimpleStorageFactory}.
 */
public class InMemorySimpleStorageFactoryTests {
    @Test
    public void testInMemorySimpleStorageFactoryCreator() {
        StorageFactoryCreator factoryCreator = new InMemoryStorageFactoryCreator();

        @Cleanup("shutdownNow")
        val executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");

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
        when(configSetup1.getConfig(any())).thenReturn(ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        val factory1 = factoryCreator.createFactory(expected[0], configSetup1, executor);
        Assert.assertTrue(factory1 instanceof InMemorySimpleStorageFactory);

        @Cleanup
        Storage storage1 = ((InMemorySimpleStorageFactory) factory1).createStorageAdapter(42, new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executor));
        Assert.assertTrue(storage1 instanceof ChunkedSegmentStorage);

        // Legacy Storage
        ConfigSetup configSetup2 = mock(ConfigSetup.class);
        when(configSetup2.getConfig(any())).thenReturn(ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        val factory2 = factoryCreator.createFactory(expected[1], configSetup2, executor);

        Assert.assertTrue(factory2 instanceof InMemoryStorageFactory);
        Storage storage2 = factory2.createStorageAdapter();
        Assert.assertTrue(storage2 instanceof AsyncStorageWrapper);

        SyncStorage syncStorage = factory2.createSyncStorage();
        Assert.assertNotNull(syncStorage);
    }

    @Test
    public void testReuse() {
        @Cleanup("shutdownNow")
        val executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        val factory = new InMemorySimpleStorageFactory(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executor, true);

        @Cleanup
        val s1 = (ChunkedSegmentStorage) factory.createStorageAdapter(42, new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executor));
        @Cleanup
        val s2 = (ChunkedSegmentStorage) factory.createStorageAdapter(42, new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executor));
        Assert.assertNotEquals("Storage should not be equal", s1, s2);
        Assert.assertEquals("ChunkStorage should be equal", s1.getChunkStorage(), s2.getChunkStorage());
    }

    @Test
    public void testNoReuse() {
        @Cleanup("shutdownNow")
        val executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        val factory = new InMemorySimpleStorageFactory(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executor, false);

        @Cleanup
        val s1 = (ChunkedSegmentStorage) factory.createStorageAdapter(42, new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executor));
        @Cleanup
        val s2 = (ChunkedSegmentStorage) factory.createStorageAdapter(42, new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executor));
        Assert.assertNotEquals("Storage should not be equal", s1, s2);
        Assert.assertNotEquals("ChunkStorage should not be equal", s1.getChunkStorage(), s2.getChunkStorage());
    }

    @Test
    public void testNull() {
        @Cleanup("shutdownNow")
        val executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        AssertExtensions.assertThrows(
                " should throw exception.",
                () -> new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, null),
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows(
                " should throw exception.",
                () -> new InMemoryMetadataStore(null, executor),
                ex -> ex instanceof NullPointerException);
    }
}
