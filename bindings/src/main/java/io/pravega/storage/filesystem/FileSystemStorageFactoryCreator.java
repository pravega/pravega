/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.filesystem;

import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import java.util.concurrent.ScheduledExecutorService;

public class FileSystemStorageFactoryCreator implements StorageFactoryCreator {

    @Override
    public String getName() {
        return "FILESYSTEM";
    }

    @Override
    public StorageFactory createFactory(ConfigSetup setup, ScheduledExecutorService executor) {
        return new FileSystemStorageFactory(setup.getConfig(FileSystemStorageConfig::builder), executor);
    }
}
