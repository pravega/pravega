/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.hdfs;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import io.pravega.segmentstore.storage.StorageFactoryInfo;
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;

import java.util.concurrent.ScheduledExecutorService;

public class HDFSStorageFactoryCreator implements StorageFactoryCreator {

    @Override
    public StorageFactoryInfo[] getStorageFactories() {
        return new StorageFactoryInfo[]{
                StorageFactoryInfo.builder()
                        .name("HDFS")
                        .storageLayoutType(StorageLayoutType.CHUNKED_STORAGE)
                        .build(),
                StorageFactoryInfo.builder()
                        .name("HDFS")
                        .storageLayoutType(StorageLayoutType.ROLLING_STORAGE)
                        .build()
        };
    }

    @Override
    public StorageFactory createFactory(StorageFactoryInfo storageFactoryInfo, ConfigSetup setup, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(storageFactoryInfo, "storageFactoryInfo");
        Preconditions.checkNotNull(setup, "setup");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkArgument(storageFactoryInfo.getName().equals("HDFS"));
        if (storageFactoryInfo.getStorageLayoutType().equals(StorageLayoutType.CHUNKED_STORAGE)) {
            return new HDFSSimpleStorageFactory(setup.getConfig(ChunkedSegmentStorageConfig::builder),
                    setup.getConfig(HDFSStorageConfig::builder),
                    executor);
        } else {
            return new HDFSStorageFactory(setup.getConfig(HDFSStorageConfig::builder), executor);
        }
    }
}
