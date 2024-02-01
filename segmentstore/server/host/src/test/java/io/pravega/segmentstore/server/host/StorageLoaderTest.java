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
package io.pravega.segmentstore.server.host;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.mocks.InMemorySimpleStorageFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.segmentstore.storage.mocks.SlowStorageFactory;
import io.pravega.segmentstore.storage.noop.ConditionalNoOpStorageFactory;
import io.pravega.segmentstore.storage.noop.NoOpStorageFactory;
import io.pravega.segmentstore.storage.noop.StorageExtraConfig;
import io.pravega.storage.extendeds3.ExtendedS3SimpleStorageFactory;
import io.pravega.storage.extendeds3.ExtendedS3StorageConfig;
import io.pravega.storage.filesystem.FileSystemSimpleStorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.storage.hdfs.HDFSSimpleStorageFactory;
import io.pravega.storage.hdfs.HDFSStorageConfig;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageLoaderTest {

    private StorageFactory expectedFactory;

    @Test
    public void testNoOpWithInMemoryStorage() throws Exception {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                .builder()
                .include(StorageExtraConfig.builder()
                        .with(StorageExtraConfig.STORAGE_NO_OP_MODE, true))
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1)
                        .with(ServiceConfig.STORAGE_IMPLEMENTATION, ServiceConfig.StorageType.INMEMORY.name()));

        ServiceBuilder builder = ServiceBuilder.newInMemoryBuilder(configBuilder.build())
                .withStorageFactory(setup -> {
                    StorageLoader loader = new StorageLoader();
                    expectedFactory = loader.load(setup, "INMEMORY", StorageLayoutType.ROLLING_STORAGE, executor);
                    return expectedFactory;
                });
        builder.initialize();
        assertTrue(expectedFactory instanceof NoOpStorageFactory);
        builder.close();

        builder = ServiceBuilder.newInMemoryBuilder(configBuilder.build())
                        .withStorageFactory(setup -> {
                           StorageLoader loader = new StorageLoader();
                           expectedFactory = loader.load(setup, "INMEMORY", StorageLayoutType.CHUNKED_STORAGE, executor);
                           return expectedFactory;
                        });
        builder.initialize();
        assertTrue(expectedFactory instanceof ConditionalNoOpStorageFactory);

        configBuilder
                .include(StorageExtraConfig.builder()
                        .with(StorageExtraConfig.STORAGE_NO_OP_MODE, false));

        builder = ServiceBuilder.newInMemoryBuilder(configBuilder.build())
                .withStorageFactory(setup -> {
                    StorageLoader loader = new StorageLoader();
                    expectedFactory = loader.load(setup, "INMEMORY", StorageLayoutType.ROLLING_STORAGE, executor);
                    return expectedFactory;
                });
        builder.initialize();
        assertTrue(expectedFactory instanceof InMemoryStorageFactory);
        builder.close();
    }

    @Test
    public void testFileSystemStorage() throws Exception {
        val storageType = ServiceConfig.StorageType.FILESYSTEM;
        ConfigSetup configSetup = mock(ConfigSetup.class);
        val extraConfig = StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_NO_OP_MODE, false)
                .build();
        when(configSetup.getConfig(any())).thenReturn(extraConfig, FileSystemStorageConfig.builder().build());
        Assert.assertNull(getStorageFactory(configSetup, storageType, "FILESYSTEM", StorageLayoutType.ROLLING_STORAGE));
    }

    @Test
    public void testSimpleFileSystemStorage() throws Exception {
        val storageType = ServiceConfig.StorageType.FILESYSTEM;
        ConfigSetup configSetup = mock(ConfigSetup.class);
        val extraConfig = StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_NO_OP_MODE, false)
                .build();
        when(configSetup.getConfig(any())).thenReturn(extraConfig, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, FileSystemStorageConfig.builder().build());

        val factory  = getStorageFactory(configSetup, storageType, "FILESYSTEM", StorageLayoutType.CHUNKED_STORAGE);
        assertTrue(factory instanceof FileSystemSimpleStorageFactory);
    }

    @Test
    public void testHDFSStorage() throws Exception {
        val storageType = ServiceConfig.StorageType.HDFS;
        ConfigSetup configSetup = mock(ConfigSetup.class);
        val extraConfig = StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_NO_OP_MODE, false)
                .build();
        when(configSetup.getConfig(any())).thenReturn(extraConfig, HDFSStorageConfig.builder().build());
        Assert.assertNull(getStorageFactory(configSetup, storageType, "HDFS", StorageLayoutType.ROLLING_STORAGE));
    }

    @Test
    public void testHDFSSimpleStorage() throws Exception {
        val storageType = ServiceConfig.StorageType.HDFS;
        ConfigSetup configSetup = mock(ConfigSetup.class);
        val extraConfig = StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_NO_OP_MODE, false)
                .build();
        when(configSetup.getConfig(any())).thenReturn(extraConfig, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, HDFSStorageConfig.builder().build());
        val factory = getStorageFactory(configSetup, storageType, "HDFS", StorageLayoutType.CHUNKED_STORAGE);
        assertTrue(factory instanceof HDFSSimpleStorageFactory);
    }

    @Test
    public void testExtendedS3Storage() throws Exception {
        val storageType = ServiceConfig.StorageType.EXTENDEDS3;
        ConfigSetup configSetup = mock(ConfigSetup.class);

        val config = ExtendedS3StorageConfig.builder()
                .with(ExtendedS3StorageConfig.CONFIGURI, "http://127.0.0.1?identity=x&secretKey=x")
                .with(ExtendedS3StorageConfig.BUCKET, "bucket")
                .with(ExtendedS3StorageConfig.PREFIX, "samplePrefix")
                .build();
        val extraConfig = StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_NO_OP_MODE, false)
                .build();
        when(configSetup.getConfig(any())).thenReturn(extraConfig, config);

        Assert.assertNull(getStorageFactory(configSetup, storageType, "EXTENDEDS3", StorageLayoutType.ROLLING_STORAGE));
    }

    @Test
    public void testExtendedS3SimpleStorage() throws Exception {
        val storageType = ServiceConfig.StorageType.EXTENDEDS3;
        ConfigSetup configSetup = mock(ConfigSetup.class);
        val config = ExtendedS3StorageConfig.builder()
                .with(ExtendedS3StorageConfig.CONFIGURI, "http://127.0.0.1?identity=x&secretKey=x")
                .with(ExtendedS3StorageConfig.BUCKET, "bucket")
                .with(ExtendedS3StorageConfig.PREFIX, "samplePrefix")
                .build();
        val extraConfig = StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_NO_OP_MODE, false)
                .build();
        when(configSetup.getConfig(any())).thenReturn(extraConfig, ChunkedSegmentStorageConfig.DEFAULT_CONFIG, config);

        val factory = getStorageFactory(configSetup, storageType, "EXTENDEDS3", StorageLayoutType.CHUNKED_STORAGE);
        assertTrue(factory instanceof ExtendedS3SimpleStorageFactory);
    }

    @Test(timeout = 120000)
    public void testSlowModeWithInMemoryStorage() throws Exception {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");

        // Case 1
        ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                .builder()
                .include(StorageExtraConfig.builder()
                        .with(StorageExtraConfig.STORAGE_SLOW_MODE, true)
                        .with(StorageExtraConfig.STORAGE_SLOW_MODE_LATENCY_MEAN, 0)
                        .with(StorageExtraConfig.STORAGE_SLOW_MODE_LATENCY_STD_DEV, 0)
                        .with(StorageExtraConfig.STORAGE_SLOW_MODE_INJECT_CHUNK_STORAGE_ONLY, false))
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1)
                        .with(ServiceConfig.STORAGE_IMPLEMENTATION, ServiceConfig.StorageType.INMEMORY.name()));

        ServiceBuilder builder = ServiceBuilder.newInMemoryBuilder(configBuilder.build())
                .withStorageFactory(setup -> {
                    StorageLoader loader = new StorageLoader();
                    expectedFactory = loader.load(setup, "INMEMORY", StorageLayoutType.CHUNKED_STORAGE, executor);
                    return expectedFactory;
                });
        builder.initialize();
        assertTrue(expectedFactory instanceof SlowStorageFactory);
        assertTrue(((SlowStorageFactory) expectedFactory).getInner() instanceof InMemorySimpleStorageFactory);
        builder.close();

        // Case 2
        configBuilder = ServiceBuilderConfig
                .builder()
                .include(StorageExtraConfig.builder()
                        .with(StorageExtraConfig.STORAGE_SLOW_MODE, true)
                        .with(StorageExtraConfig.STORAGE_SLOW_MODE_LATENCY_MEAN, 0)
                        .with(StorageExtraConfig.STORAGE_SLOW_MODE_LATENCY_STD_DEV, 0)
                        .with(StorageExtraConfig.STORAGE_SLOW_MODE_INJECT_CHUNK_STORAGE_ONLY, true))
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1)
                        .with(ServiceConfig.STORAGE_IMPLEMENTATION, ServiceConfig.StorageType.INMEMORY.name()));

        builder = ServiceBuilder.newInMemoryBuilder(configBuilder.build())
                .withStorageFactory(setup -> {
                    StorageLoader loader = new StorageLoader();
                    expectedFactory = loader.load(setup, "INMEMORY", StorageLayoutType.CHUNKED_STORAGE, executor);
                    return expectedFactory;
                });
        builder.initialize();
        assertTrue(expectedFactory instanceof SlowStorageFactory);
        assertTrue(((SlowStorageFactory) expectedFactory).getInner() instanceof InMemorySimpleStorageFactory);
        builder.close();

        // Case 3
        configBuilder
                .include(StorageExtraConfig.builder()
                        .with(StorageExtraConfig.STORAGE_SLOW_MODE, false));

        builder = ServiceBuilder.newInMemoryBuilder(configBuilder.build())
                .withStorageFactory(setup -> {
                    StorageLoader loader = new StorageLoader();
                    expectedFactory = loader.load(setup, "INMEMORY", StorageLayoutType.CHUNKED_STORAGE, executor);
                    return expectedFactory;
                });
        builder.initialize();
        assertTrue(expectedFactory instanceof InMemorySimpleStorageFactory);
        builder.close();
    }

    private StorageFactory getStorageFactory(ConfigSetup setup, ServiceConfig.StorageType storageType, String name, StorageLayoutType storageLayoutType) {
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        StorageLoader loader = new StorageLoader();
        return loader.load(setup, name, storageLayoutType, executor);
    }
}