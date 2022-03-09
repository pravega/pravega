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

import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.noop.StorageExtraConfig;
import io.pravega.segmentstore.storage.rolling.RollingStorageTests;
import lombok.val;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Unit tests for {@link SlowStorage}
 */
public class SlowStorageTests extends StorageTestBase {

    @Override
    public void testFencing() throws Exception {

    }

    @Override
    protected Storage createStorage() throws Exception {
        return getSlowStorage(executorService());
    }

    private static SlowStorage getSlowStorage(ScheduledExecutorService executor) {
        val chunkStorage = new InMemoryChunkStorage(executor);
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        val metaDataStore = new InMemoryMetadataStore(config, executor);
        val innerStorage = new ChunkedSegmentStorage(10, chunkStorage, metaDataStore, executor, config);
        // We do not need to call bootstrap method here. We can just initialize garbageCollector directly.
        innerStorage.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();
        return new SlowStorage(innerStorage, executor, StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_SLOW_MODE, true)
                .with(StorageExtraConfig.STORAGE_SLOW_MODE_DISTRIBUTION_TYPE, StorageDelayDistributionType.NORMAL_DISTRIBUTION_TYPE)
                .with(StorageExtraConfig.STORAGE_SLOW_MODE_LATENCY_STD_DEV, 0)
                .with(StorageExtraConfig.STORAGE_SLOW_MODE_LATENCY_MEAN, 0)
                .build());
    }

    /**
     * Extending StorageTestBase implementation
     */
    public static class SlowRollingTests extends RollingStorageTests {
        @Override
        protected Storage createStorage() {
            useOldLayout = false;
            return getSlowStorage(executorService());
        }
    }
}
