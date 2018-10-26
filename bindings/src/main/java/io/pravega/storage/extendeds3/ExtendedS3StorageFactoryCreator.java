/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.extendeds3;

import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import java.util.concurrent.ScheduledExecutorService;

public class ExtendedS3StorageFactoryCreator implements StorageFactoryCreator {
    @Override
    public StorageFactory createFactory(ConfigSetup setup, ScheduledExecutorService executor) {
        return new ExtendedS3StorageFactory(setup.getConfig(ExtendedS3StorageConfig::builder), executor);
    }

    @Override
    public String getName() {
        return "EXTENDEDS3";
    }
}
