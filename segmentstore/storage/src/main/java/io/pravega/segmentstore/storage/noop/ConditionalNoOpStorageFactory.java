package io.pravega.segmentstore.storage.noop;

import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.chunklayer.ChunkStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import io.pravega.segmentstore.storage.mocks.SlowChunkStorage;
import io.pravega.segmentstore.storage.mocks.SlowStorage;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.val;

import java.util.concurrent.ScheduledExecutorService;

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
     * @return
     */
    @Override
    public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
        ChunkedSegmentStorage chunkedSegmentStorage = new ChunkedSegmentStorage(containerId,
                createChunkStorage(),
                metadataStore,
                this.executor,
                getChunkedSegmentStorageConfig());
        return chunkedSegmentStorage;
    }

    @Override
    public Storage createStorageAdapter() {
        return new SlowStorage(inner.createStorageAdapter(), executor, config);
    }

    @Override
    public ChunkedSegmentStorageConfig getChunkedSegmentStorageConfig() {
            return inner.getChunkedSegmentStorageConfig();
    }

    @Override
    public ChunkStorage createChunkStorage() {
            val innerChunkStorage = ((SimpleStorageFactory) inner).createChunkStorage();
            return new SlowChunkStorage(innerChunkStorage, executor, config);
    }
}
