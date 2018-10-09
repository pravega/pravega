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
import io.pravega.segmentstore.storage.StorageFactoryFactory;
import java.util.ServiceLoader;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StorageLoader {
    public StorageFactory load(ConfigSetup setup, String storageImplementation, ScheduledExecutorService executor) {
        ServiceLoader<StorageFactoryFactory> loader = ServiceLoader.load(StorageFactoryFactory.class);
        for (StorageFactoryFactory factory : loader) {
            log.info("Loading {}, trying {}", storageImplementation, factory.getName());
            if (factory.getName().equals(storageImplementation)) {
                return factory.createFactory(setup, executor);
            }
        }
        return null;
    }
}
