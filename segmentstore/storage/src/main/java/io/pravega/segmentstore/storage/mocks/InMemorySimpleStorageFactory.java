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

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import lombok.Getter;

import java.util.concurrent.ScheduledExecutorService;

/**
 * In-Memory mock for StorageFactory. Contents is destroyed when object is garbage collected.
 */
public class InMemorySimpleStorageFactory implements SimpleStorageFactory {
    @Getter
    protected ChunkedSegmentStorageConfig chunkedSegmentStorageConfig;

    @Getter
    protected ScheduledExecutorService executor;

    private ChunkStorage singletonChunkStorage;
    private boolean reuseStorage;

    public InMemorySimpleStorageFactory(ChunkedSegmentStorageConfig config, ScheduledExecutorService executor, boolean reuseStorage) {
        this.chunkedSegmentStorageConfig = Preconditions.checkNotNull(config, "config");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.reuseStorage = reuseStorage;
    }

    @Override
    synchronized public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
        return newStorage(containerId, executor, createChunkStorage(), metadataStore);
    }

    /**
     * Creates a new instance of a Storage adapter.
     */
    @Override
    public Storage createStorageAdapter() {
        throw new UnsupportedOperationException("SimpleStorageFactory requires ChunkMetadataStore");
    }

    @Override
    synchronized public ChunkStorage createChunkStorage() {
        if (null != singletonChunkStorage) {
            return singletonChunkStorage;
        }
        ChunkStorage chunkStorage = new InMemoryChunkStorage(executor);
        if (reuseStorage) {
            singletonChunkStorage = chunkStorage;
        }
        return chunkStorage;
    }

    static Storage newStorage(int containerId, ScheduledExecutorService executor, ChunkStorage chunkStorage, ChunkMetadataStore metadataStore) {
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(containerId,
                chunkStorage,
                metadataStore,
                executor,
                ChunkedSegmentStorageConfig.DEFAULT_CONFIG);
        chunkedSegmentStorage.initialize(1);
        return chunkedSegmentStorage;
    }
}
