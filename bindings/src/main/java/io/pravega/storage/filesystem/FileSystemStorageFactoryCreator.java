/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.filesystem;

import com.google.common.base.Preconditions;
import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import io.pravega.segmentstore.storage.StorageFactoryInfo;
import io.pravega.segmentstore.storage.StorageManagerLayoutType;
import io.pravega.segmentstore.storage.StorageManagerType;

import java.util.concurrent.ScheduledExecutorService;

public class FileSystemStorageFactoryCreator implements StorageFactoryCreator {

    @Override
    public StorageFactoryInfo[] getStorageFactories() {
        return new StorageFactoryInfo[]{
                StorageFactoryInfo.builder()
                        .name("FILESYSTEM")
                        .storageManagerLayoutType(StorageManagerLayoutType.TABLE_BASED)
                        .storageManagerType(StorageManagerType.CHUNK_MANAGER)
                        .build(),
                StorageFactoryInfo.builder()
                        .name("FILESYSTEM")
                        .storageManagerLayoutType(StorageManagerLayoutType.LEGACY)
                        .storageManagerType(StorageManagerType.NONE)
                        .build()
        };
    }

    @Override
    public StorageFactory createFactory(StorageFactoryInfo storageFactoryInfo, ConfigSetup setup, ScheduledExecutorService executor) {
        Preconditions.checkNotNull(storageFactoryInfo, "storageFactoryInfo");
        Preconditions.checkNotNull(setup, "setup");
        Preconditions.checkNotNull(executor, "executor");
        Preconditions.checkArgument(storageFactoryInfo.getName().equals("FILESYSTEM"));
        if (storageFactoryInfo.getStorageManagerType().equals(StorageManagerType.CHUNK_MANAGER)) {
            return new FileSystemSimpleStorageFactory(setup.getConfig(FileSystemStorageConfig::builder), executor);
        } else {
            return new FileSystemStorageFactory(setup.getConfig(FileSystemStorageConfig::builder), executor);
        }
    }
}
