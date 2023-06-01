/**
 * Copyright Pravega Authors.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.noop;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.segmentstore.storage.mocks.InMemorySimpleStorageFactory;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Unit tests for {@link ConditionalNoOpStorageFactory} using {@link SimpleStorageTests}.
 */
public class ConditionalNoOpStorageFactoryTest {

    @Test
    public void testCreateSyncStorage() {
        StorageExtraConfig config = StorageExtraConfig.builder().build();
        @Cleanup("shutdownNow")
        ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(1, "test");
        var factory = new ConditionalNoOpStorageFactory(executor, new InMemorySimpleStorageFactory(ChunkedSegmentStorageConfig.DEFAULT_CONFIG,
                executor, false), config);

        @Cleanup
        var storage = factory.createStorageAdapter(43, new InMemoryMetadataStore(ChunkedSegmentStorageConfig.DEFAULT_CONFIG, executor));
        Assert.assertTrue(storage instanceof ChunkedSegmentStorage);
        Assert.assertTrue(((ChunkedSegmentStorage) storage).getChunkStorage() instanceof ConditionalNoOpChunkStorage);
        ((ChunkedSegmentStorage) storage).getChunkStorage().report();

        if (!((ChunkedSegmentStorage) storage).getChunkStorage().supportsConcat()) {
            AssertExtensions.assertThrows("storage.concat should not succed.",
                    () -> ((ChunkedSegmentStorage) storage).getChunkStorage().concat(null),
                    ex -> ex instanceof UnsupportedOperationException);
        }

        AssertExtensions.assertThrows(
                "factory.createSyncStorage should not succeed.",
                () -> factory.createSyncStorage(),
                ex -> ex instanceof UnsupportedOperationException);

        AssertExtensions.assertThrows(
                "factory.createSyncStorage should not succeed.",
                () -> factory.createStorageAdapter(),
                ex -> ex instanceof UnsupportedOperationException);
    }
}
