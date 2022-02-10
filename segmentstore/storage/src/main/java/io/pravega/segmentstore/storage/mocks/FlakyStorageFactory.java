package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.metadata.ChunkMetadataStore;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

@RequiredArgsConstructor

public class FlakyStorageFactory implements SimpleStorageFactory {
    @Getter
    final protected ChunkedSegmentStorageConfig chunkedSegmentStorageConfig;

    @Getter
    final protected ScheduledExecutorService executor;

    @Getter
    final protected SimpleStorageFactory inner;

    @Getter
    final protected Duration duration;

    @Override
    public Storage createStorageAdapter(int containerId, ChunkMetadataStore metadataStore) {
        return new FlakyStorage(inner.createStorageAdapter(containerId, metadataStore),executor, duration);
    }

    @Override
    public Storage createStorageAdapter() {
        return new FlakyStorage(inner.createStorageAdapter(),executor, duration);
    }
}
