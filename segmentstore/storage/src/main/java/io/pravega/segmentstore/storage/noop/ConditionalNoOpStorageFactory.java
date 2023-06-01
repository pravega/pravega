/**
 * Copyright Pravega Authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.storage.noop;

import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Creates an instance of {@link  ConditionalNoOpStorageFactory} and delegates calls to inner {@link SimpleStorageFactory}.
 */
@RequiredArgsConstructor
public class ConditionalNoOpStorageFactory implements SimpleStorageFactory {

    @Getter
    final private ScheduledExecutorService executor;

    @Getter
    final private SimpleStorageFactory inner;

    @Getter
    final private StorageExtraConfig config;

    /**
     * Creates an instance of Storage.
     *
     * @param containerId   Container ID.
     * @param metadataStore {@link ChunkMetadataStore} store to use.
     * @return returns Storage
     */
    @Override
    public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
        return new ChunkedSegmentStorage(containerId,
                createChunkStorage(),
                metadataStore,
                this.executor,
                getChunkedSegmentStorageConfig());
    }

    @Override
    public Storage createStorageAdapter() {
        throw new UnsupportedOperationException("createStorageAdapter method is not supported for ConditionalNoOpStorageFactory, use the parameterized method instead.");

    }

    @Override
    public ChunkedSegmentStorageConfig getChunkedSegmentStorageConfig() {
        return inner.getChunkedSegmentStorageConfig();
    }

    @Override
    public ChunkStorage createChunkStorage() {
        val innerChunkStorage = inner.createChunkStorage();
        return new ConditionalNoOpChunkStorage(innerChunkStorage, executor);
    }
}
