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

import io.pravega.segmentstore.storage.chunklayer.ChunkedRollingStorageTests;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorageTests;
import io.pravega.segmentstore.storage.chunklayer.SimpleStorageTests;
import io.pravega.segmentstore.storage.noop.StorageExtraConfig;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Unit tests for {@link SlowChunkStorage} using {@link SimpleStorageTests}.
 */
public class SlowChunkStorageTests extends SimpleStorageTests {
    @Override
    protected ChunkStorage getChunkStorage() {
        return getSlowChunkStorage(executorService());
    }

    static SlowChunkStorage getSlowChunkStorage(ScheduledExecutorService executorService) {
        ChunkStorage inner = new InMemoryChunkStorage(executorService);
        return new SlowChunkStorage(inner, executorService, StorageExtraConfig.builder()
                .with(StorageExtraConfig.STORAGE_SLOW_MODE, true)
                .with(StorageExtraConfig.STORAGE_SLOW_MODE_DISTRIBUTION_TYPE, StorageDelayDistributionType.NORMAL_DISTRIBUTION_TYPE)
                .with(StorageExtraConfig.STORAGE_SLOW_MODE_LATENCY_STD_DEV, 0)
                .with(StorageExtraConfig.STORAGE_SLOW_MODE_LATENCY_MEAN, 0)
                .build());
    }

    /*
     * Unit tests for {@link SlowChunkStorage} using {@link ChunkedRollingStorageTests}.
     */
    public static class SlowChunkStorageRollingStorageTests extends ChunkedRollingStorageTests {
        @Override
        protected ChunkStorage getChunkStorage() {
            return getSlowChunkStorage(executorService());
        }
    }

    /**
     * Unit tests for {@link SlowChunkStorage} using {@link ChunkStorageTests}.
     */
    public static class SlowChunkStorageTest extends ChunkStorageTests {
        @Override
        protected ChunkStorage createChunkStorage() {
            return getSlowChunkStorage(executorService());
        }
    }
}
