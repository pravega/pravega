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

import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import io.pravega.segmentstore.storage.StorageFactoryInfo;

import java.util.concurrent.ScheduledExecutorService;

/**
 * {@link StorageFactoryCreator} for creating an HDFS based {@link io.pravega.segmentstore.storage.Storage}.
 */
public class HDFSSimpleStorageFactoryCreator implements StorageFactoryCreator {

    @Override
    public StorageFactoryInfo getStorageFactoryInfo() {
        return StorageFactoryInfo.builder()
                .name("HDFS")
                .chunkManagerSupported(true)
                .legacyLayoutSupported(false)
                .build();
    }

    @Override
    public StorageFactory createFactory(ConfigSetup setup, ScheduledExecutorService executor) {
        return new HDFSSimpleStorageFactory(setup.getConfig(HDFSStorageConfig::builder), executor);
    }
}
