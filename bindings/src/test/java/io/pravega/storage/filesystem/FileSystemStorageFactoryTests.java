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
package io.pravega.storage.filesystem;

import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import io.pravega.segmentstore.storage.StorageFactoryInfo;
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test factories.
 */
public class FileSystemStorageFactoryTests extends ThreadPooledTestSuite {
    @Test
    public void testFileSystemStorageFactoryCreator() {
        StorageFactoryCreator factoryCreator = new FileSystemStorageFactoryCreator();
        val expected = new StorageFactoryInfo[]{
                StorageFactoryInfo.builder()
                        .name("FILESYSTEM")
                        .storageLayoutType(StorageLayoutType.CHUNKED_STORAGE)
                        .build()
        };

        val factoryInfoList = factoryCreator.getStorageFactories();
        Assert.assertEquals(1, factoryInfoList.length);
        Assert.assertArrayEquals(expected, factoryInfoList);

        // Simple Storage
        ConfigSetup configSetup1 = mock(ConfigSetup.class);
        when(configSetup1.getConfig(any())).thenReturn(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, FileSystemStorageConfig.builder().build());
        val factory1 = factoryCreator.createFactory(expected[0], configSetup1, executorService());
        Assert.assertTrue(factory1 instanceof FileSystemSimpleStorageFactory);

        Assert.assertEquals(executorService(), ((FileSystemSimpleStorageFactory) factory1).getExecutor());
        Assert.assertEquals(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, ((FileSystemSimpleStorageFactory) factory1).getChunkedSegmentStorageConfig());

        // Rolling Storage
        AssertExtensions.assertThrows(
                "createFactory should throw UnsupportedOperationException.",
                () -> factoryCreator.createFactory(StorageFactoryInfo.builder()
                        .name("FILESYSTEM")
                        .storageLayoutType(StorageLayoutType.ROLLING_STORAGE)
                        .build(), configSetup1, executorService()),
                ex -> ex instanceof UnsupportedOperationException);

        @Cleanup
        Storage storage1 = ((FileSystemSimpleStorageFactory) factory1).createStorageAdapter(42, new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executorService()));
        Assert.assertTrue(storage1 instanceof ChunkedSegmentStorage);
        Assert.assertTrue(((ChunkedSegmentStorage) storage1).getChunkStorage() instanceof FileSystemChunkStorage);

        AssertExtensions.assertThrows(
                "createStorageAdapter should throw UnsupportedOperationException.",
                () -> factory1.createStorageAdapter(),
                ex -> ex instanceof UnsupportedOperationException);
    }

    @Test
    public void testNull() {
        AssertExtensions.assertThrows(
                " should throw exception.",
                () -> new FileSystemSimpleStorageFactory(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, null, executorService()),
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows(
                " should throw exception.",
                () -> new FileSystemSimpleStorageFactory(null, FileSystemStorageConfig.builder().build(), executorService()),
                ex -> ex instanceof NullPointerException);
        AssertExtensions.assertThrows(
                " should throw exception.",
                () -> new FileSystemSimpleStorageFactory(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, FileSystemStorageConfig.builder().build(), null),
                ex -> ex instanceof NullPointerException);
    }
}
