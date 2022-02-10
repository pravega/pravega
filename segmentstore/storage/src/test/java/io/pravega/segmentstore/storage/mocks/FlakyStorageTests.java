package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageTestBase;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorage;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.rolling.RollingStorageTests;
import lombok.val;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

public class FlakyStorageTests extends StorageTestBase {

    @Override
    public void testFencing() throws Exception {

    }

    @Override
    protected Storage createStorage() throws Exception {
        return getFlakyStorage(executorService());
    }

    private static FlakyStorage getFlakyStorage(ScheduledExecutorService executor) {
        val chunkStorage = new InMemoryChunkStorage(executor);
        val config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        val metaDataStore = new InMemoryMetadataStore(config, executor);
        val innerStorage = new ChunkedSegmentStorage(10, chunkStorage, metaDataStore, executor, config);
        // We do not need to call bootstrap method here. We can just initialize garbageCollector directly.
        innerStorage.getGarbageCollector().initialize(new InMemoryTaskQueueManager()).join();
        return new FlakyStorage(innerStorage, executor, Duration.ZERO);
    }

    public static class FlakyRollingTests extends RollingStorageTests {
        @Override
        protected Storage createStorage() {
            useOldLayout = false;
            return getFlakyStorage(executorService());
        }
    }
}
