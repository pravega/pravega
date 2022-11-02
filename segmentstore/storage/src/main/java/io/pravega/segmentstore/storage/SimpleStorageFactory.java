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
package io.pravega.segmentstore.storage;

import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;

import java.util.concurrent.Executor;

/**
 * Defines a Factory that creates an instance of {@link io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage}.
 */
public interface SimpleStorageFactory extends StorageFactory {
    /**
     * Creates a new instance of a Storage adapter.
     * @param containerId Container ID.
     * @param metadataStore {@link ChunkMetadataStore} store to use.
     */
    Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore);

    /**
     * Gets the executor used by the factory.
     * @return Executor used by the factory.
     */
    Executor getExecutor();

    /**
     * Gets the ChunkedSegmentStorageConfig used by the factory.
     * @return ChunkedSegmentStorageConfig used by the factory.
     */
    ChunkedSegmentStorageConfig getChunkedSegmentStorageConfig();

    /**
     * Creates a new instance of {@link ChunkStorage}.
     */
    ChunkStorage createChunkStorage();
}
