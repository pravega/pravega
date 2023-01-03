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
package io.pravega.segmentstore.storage.chunklayer;

import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.mocks.InMemoryChunkStorage;
import io.pravega.segmentstore.storage.mocks.InMemoryMetadataStore;
import io.pravega.segmentstore.storage.mocks.InMemoryTaskQueueManager;
import io.pravega.segmentstore.storage.rolling.RollingStorageTestBase;
import lombok.val;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Unit tests for  {@link ChunkedSegmentStorage} and {@link ChunkStorage} based implementation that exercise scenarios
 * for {@link io.pravega.segmentstore.storage.rolling.RollingStorage}.
 */
public abstract class ChunkedRollingStorageTests extends RollingStorageTestBase {
    private static final int CONTAINER_ID = 42;

    ChunkStorage chunkStorage;
    ChunkMetadataStore chunkMetadataStore;

    /**
     * Creates a new instance of the Storage implementation to be tested. This will be cleaned up (via close()) upon
     * test termination.
     */
    @Override
    protected Storage createStorage() throws Exception {
        useOldLayout = false;
        ScheduledExecutorService executor = executorService();
        // Initialize
        synchronized (ChunkedRollingStorageTests.class) {
            if (null == chunkStorage) {
                chunkMetadataStore = getMetadataStore();
                chunkStorage = getChunkStorage();
            }
        }
        val ret = new ChunkedSegmentStorage(CONTAINER_ID,
                chunkStorage,
                chunkMetadataStore,
                executor,
                getDefaultConfig());
        ret.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();
        return ret;
    }

    protected ChunkedSegmentStorageConfig getDefaultConfig() {
        return ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
    }

    /**
     * Creates a ChunkStorage.
     *
     * @return ChunkStorage.
     * @throws Exception If any unexpected error occurred.
     */
    protected ChunkStorage getChunkStorage() throws Exception {
        return new InMemoryChunkStorage(executorService());
    }

    /**
     * Creates a ChunkMetadataStore.
     *
     * @return ChunkMetadataStore
     * @throws Exception If any unexpected error occurred.
     */
    protected ChunkMetadataStore getMetadataStore() throws Exception {
        return new InMemoryMetadataStore(getDefaultConfig(), executorService());
    }
}
