package io.pravega.storage.chunk;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.*;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;

import java.util.concurrent.ScheduledExecutorService;

public class ECSChunkStorageFactoryCreator implements StorageFactoryCreator {
    @Override
    public StorageFactory createFactory(StorageFactoryInfo storageFactoryInfo, ConfigSetup setup, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(storageFactoryInfo, "storageFactoryInfo");
        Preconditions.checkNotNull(setup, "setup");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkArgument(storageFactoryInfo.getName().equals("ECSCHUNK"));
        return new ECSChunkSimpleStorageFactory(setup.getConfig(ChunkedSegmentStorageConfig::builder),
                setup.getConfig(ECSChunkStorageConfig::builder),
                executor);
    }

    @Override
    public StorageFactoryInfo[] getStorageFactories() {
        return new StorageFactoryInfo[]{
                StorageFactoryInfo.builder()
                        .name("ECSCHUNK")
                        .storageLayoutType(StorageLayoutType.CHUNKED_STORAGE)
                        .build(),
        };
    }
}
