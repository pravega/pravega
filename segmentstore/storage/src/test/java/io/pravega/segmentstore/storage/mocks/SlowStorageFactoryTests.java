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
import io.pravega.common.util.Property;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.noop.StorageExtraConfig;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link SlowStorageFactory}.
 */
public class SlowStorageFactoryTests {
    @Test
    public void testSlowStorageFactoryWithChunkedSegmentStorage() {

        @Cleanup("shutdownNow")
        val executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        SimpleStorageFactory innerFactory = new InMemorySimpleStorageFactory(ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executor, false);
        SlowStorageFactory factory = new SlowStorageFactory(executor, innerFactory, StorageExtraConfig.builder()
                .with(Property.named("slow.inject.chunk.storage"), false)
                .build());

        @Cleanup
        SlowStorage storage1 = (SlowStorage) factory.createStorageAdapter(42, new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executor));
        Assert.assertTrue(storage1.getInner() instanceof ChunkedSegmentStorage);
        val chunkedSegmentStorage = (ChunkedSegmentStorage) storage1.getInner();
        Assert.assertTrue(chunkedSegmentStorage.getChunkStorage() instanceof InMemoryChunkStorage);
        Assert.assertEquals(factory.getChunkedSegmentStorageConfig(), innerFactory.getChunkedSegmentStorageConfig());

        AssertExtensions.assertThrows(
                "factory.createStorageAdapter should not succeed.",
                () -> factory.createStorageAdapter(),
                ex -> ex instanceof UnsupportedOperationException);

        AssertExtensions.assertThrows(
                "factory.createSyncStorage should not succeed.",
                () -> factory.createSyncStorage(),
                ex -> ex instanceof UnsupportedOperationException);
    }

    @Test
    public void testSlowStorageFactoryWithChunkedSegmentStorageWithChunkStorageOnly() {

        @Cleanup("shutdownNow")
        val executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        SimpleStorageFactory innerFactory = new InMemorySimpleStorageFactory(ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executor, false);
        SlowStorageFactory factory = new SlowStorageFactory(executor, innerFactory, StorageExtraConfig.builder().build());

        @Cleanup
        ChunkedSegmentStorage storage1 = (ChunkedSegmentStorage) factory.createStorageAdapter(42, new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executor));
        Assert.assertTrue(storage1.getChunkStorage() instanceof SlowChunkStorage);
        val chunkStorage = (SlowChunkStorage) storage1.getChunkStorage();
        Assert.assertTrue(chunkStorage.getInner() instanceof InMemoryChunkStorage);
        Assert.assertEquals(factory.getChunkedSegmentStorageConfig(), innerFactory.getChunkedSegmentStorageConfig());

        AssertExtensions.assertThrows(
                "factory.createStorageAdapter should not succeed.",
                () -> factory.createStorageAdapter(),
                ex -> ex instanceof UnsupportedOperationException);

        AssertExtensions.assertThrows(
                "factory.createSyncStorage should not succeed.",
                () -> factory.createSyncStorage(),
                ex -> ex instanceof UnsupportedOperationException);
    }

    @Test
    public void testSlowStorageFactoryWithRollingStorage() {

        @Cleanup("shutdownNow")
        val executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        StorageFactory innerFactory = new InMemoryStorageFactory(executor);
        SlowStorageFactory factory = new SlowStorageFactory(executor, innerFactory, StorageExtraConfig.builder().build());

        AssertExtensions.assertThrows(
                "factory.createStorageAdapter should not succeed.",
                () -> factory.createStorageAdapter(42, new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executor)),
                ex -> ex instanceof UnsupportedOperationException);

        AssertExtensions.assertThrows(
                "factory.getChunkedSegmentStorageConfig should not succeed.",
                () -> factory.getChunkedSegmentStorageConfig(),
                ex -> ex instanceof UnsupportedOperationException);

        @Cleanup
        SlowStorage storage1 = (SlowStorage) factory.createStorageAdapter();
        Assert.assertTrue(storage1.getInner() instanceof AsyncStorageWrapper);

        SyncStorage syncStorage = factory.createSyncStorage();
        Assert.assertTrue(syncStorage instanceof InMemoryStorage);
    }
}
