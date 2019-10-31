/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import java.util.ServiceLoader;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;

/**
 * This class loads a specific storage implementation dynamically. It uses the `ServiceLoader` (https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html)
 * for this purpose.
 * The custom storage implementation is required to implement {@link StorageFactoryCreator} interface.
 *
 */
@Slf4j
public class StorageLoader {
    public StorageFactory load(ConfigSetup setup, String storageImplementation, ScheduledExecutorService executor) {
        ServiceLoader<StorageFactoryCreator> loader = ServiceLoader.load(StorageFactoryCreator.class);
        for (StorageFactoryCreator factory : loader) {
            log.info("Loading {}, trying {}", storageImplementation, factory.getName());
            if (factory.getName().equals(storageImplementation)) {
                return factory.createFactory(setup, executor);
            }
        }
        return null;
    }
}
