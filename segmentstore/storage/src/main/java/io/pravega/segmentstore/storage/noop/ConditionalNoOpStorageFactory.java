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
    final protected ScheduledExecutorService executor;

    @Getter
    final protected SimpleStorageFactory inner;

    @Getter
    final protected StorageExtraConfig config;

    /**
     * Creates an instance of Storage.
     * @param containerId Container ID.
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
