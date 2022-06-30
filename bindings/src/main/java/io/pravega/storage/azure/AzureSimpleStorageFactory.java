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
package io.pravega.storage.azure;

import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Factory for Azure {@link Storage} implemented using {@link ChunkedSegmentStorage} and {@link AzureChunkStorage}.
 */
@RequiredArgsConstructor
public class AzureSimpleStorageFactory implements SimpleStorageFactory {

    @NonNull
    @Getter
    private final ChunkedSegmentStorageConfig chunkedSegmentStorageConfig;

    @NonNull
    private final AzureStorageConfig config;

    @NonNull
    @Getter
    private final ScheduledExecutorService executor;

    @Override
    public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(containerId,
                createChunkStorage(),
                metadataStore,
                this.executor,
                this.chunkedSegmentStorageConfig);
        return chunkedSegmentStorage;
    }

    /**
     * Creates a new instance of a Storage adapter.
     */
    @Override
    public Storage createStorageAdapter() {
        throw new UnsupportedOperationException("SimpleStorageFactory requires ChunkMetadataStore");
    }

    @Override
    public ChunkStorage createChunkStorage() {
        AzureClient azureClient = createAzureClient(this.config);
        return new AzureChunkStorage(azureClient, this.config, this.executor, true);
    }

    private AzureClient createAzureClient(AzureStorageConfig config) {
        return new AzureBlobClientImpl(config);
    }
}
