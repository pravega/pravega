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

import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.SyncStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.noop.StorageExtraConfig;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Creates an instance of {@link  SlowStorage} and delegates calls to inner {@link SimpleStorageFactory}.
 */
@RequiredArgsConstructor
public class SlowStorageFactory implements SimpleStorageFactory {
    @Getter
    final protected ScheduledExecutorService executor;

    @Getter
    final protected StorageFactory inner;

    @Getter
    final protected StorageExtraConfig config;

    /**
     * Creates an instance of Storage.
     * @param containerId Container ID.
     * @param metadataStore {@link ChunkMetadataStore} store to use.
     * @return
     */
    @Override
    public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
        if (inner instanceof SimpleStorageFactory) {
            if (config.isSlowModeInjectChunkStorageOnly()) {
                ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(containerId,
                        createChunkStorage(),
                        metadataStore,
                        this.executor,
                        getChunkedSegmentStorageConfig());
                return chunkedSegmentStorage;
            } else {
                val innerStorage = ((SimpleStorageFactory) inner).createStorageAdapter(containerId, metadataStore);
                return new SlowStorage(innerStorage, executor, config);
            }
        } else {
            throw new UnsupportedOperationException("inner is not SimpleStorageFactory");
        }
    }

    @Override
    public Storage createStorageAdapter() {
        return new SlowStorage(inner.createStorageAdapter(), executor, config);
    }

    @Override
    public ChunkedSegmentStorageConfig getChunkedSegmentStorageConfig() {
        if (inner instanceof SimpleStorageFactory) {
            return ((SimpleStorageFactory) inner).getChunkedSegmentStorageConfig();
        } else {
            throw new UnsupportedOperationException("inner is not SimpleStorageFactory");
        }
    }

    @Override
    public ChunkStorage createChunkStorage() {
        if (inner instanceof SimpleStorageFactory && config.isSlowModeInjectChunkStorageOnly()) {
            val innerChunkStorage = ((SimpleStorageFactory) inner).createChunkStorage();
            return new SlowChunkStorage(innerChunkStorage, executor, config);
        } else {
            throw new UnsupportedOperationException("inner is not SimpleStorageFactory");
        }
    }

    @Override
    public SyncStorage createSyncStorage() {
        if (inner instanceof SimpleStorageFactory) {
            throw new UnsupportedOperationException("SimpleStorageFactory does not support createSyncStorage");
        } else {
            return inner.createSyncStorage();
        }
    }
}
