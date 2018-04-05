/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import io.pravega.common.ConfigSetup;
import io.pravega.segmentstore.storage.StorageFactory;
import java.util.ServiceLoader;
import java.util.concurrent.ScheduledExecutorService;

public class StorageLoader {
    public StorageFactory load(ConfigSetup setup, String storageImplementation, ScheduledExecutorService executor) {
        ServiceLoader<StorageFactory> loader = ServiceLoader.load(StorageFactory.class);
        for (StorageFactory factory : loader) {
            if (factory.getName().equals(storageImplementation)) {
                factory.initialize(setup, executor);
                return factory;
            }
        }
        return null;
    }
}
