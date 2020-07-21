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

import java.util.ServiceLoader;
import java.util.concurrent.ScheduledExecutorService;

import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import io.pravega.segmentstore.storage.StorageMetadataFormat;
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.noop.StorageExtraConfig;
import io.pravega.segmentstore.storage.noop.NoOpStorageFactory;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

/**
 * This class loads a specific storage implementation dynamically. It uses the `ServiceLoader` (https://docs.oracle.com/javase/7/docs/api/java/util/ServiceLoader.html)
 * for this purpose.
 * The custom storage implementation is required to implement {@link StorageFactoryCreator} interface.
 *
 * If storageextra.storageNoOpMode is set to true, then instance of NoOpStorageFactory is returned which wraps the storage implementation factory.
 */
@Slf4j
public class StorageLoader {
    public StorageFactory load(ConfigSetup setup,
                               String storageImplementation,
                               StorageLayoutType storageLayoutType,
                               StorageMetadataFormat storageMetadataFormat,
                               ScheduledExecutorService executor) {
        ServiceLoader<StorageFactoryCreator> loader = ServiceLoader.load(StorageFactoryCreator.class);
        StorageExtraConfig noOpConfig = setup.getConfig(StorageExtraConfig::builder);
        for (StorageFactoryCreator factoryCreator : loader) {
            val factories = factoryCreator.getStorageFactories();
            for (val factoryInfo : factories) {
                log.info("Loading {}, trying {}", storageImplementation, factoryInfo);
                if (factoryInfo.getName().equals(storageImplementation)
                        && factoryInfo.getStorageMetadataFormat() == storageMetadataFormat
                        && factoryInfo.getStorageLayoutType() == storageLayoutType) {
                    StorageFactory factory = factoryCreator.createFactory(factoryInfo, setup, executor);
                    if (!noOpConfig.isStorageNoOpMode()) {
                        return factory;
                    } else { //The specified storage implementation is in No-Op mode.
                        log.warn("{} IS IN NO-OP MODE: DATA LOSS WILL HAPPEN! MAKE SURE IT IS BY FULL INTENTION FOR TESTING PURPOSE!", storageImplementation);
                        return new NoOpStorageFactory(noOpConfig, executor, factory, null);
                    }
                }
            }
        }
        return null;
    }
}
