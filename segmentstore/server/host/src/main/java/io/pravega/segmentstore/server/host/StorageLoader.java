/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.segmentstore.server.host;

import io.pravega.segmentstore.storage.ConfigSetup;
import io.pravega.segmentstore.storage.SimpleStorageFactory;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.StorageFactoryCreator;
import io.pravega.segmentstore.storage.StorageLayoutType;
import io.pravega.segmentstore.storage.mocks.SlowStorageFactory;
import io.pravega.segmentstore.storage.noop.ConditionalNoOpStorageFactory;
import io.pravega.segmentstore.storage.noop.NoOpStorageFactory;
import io.pravega.segmentstore.storage.noop.StorageExtraConfig;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.ServiceLoader;
import java.util.concurrent.ScheduledExecutorService;

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
                               ScheduledExecutorService executor) {
        ServiceLoader<StorageFactoryCreator> loader = ServiceLoader.load(StorageFactoryCreator.class);
        StorageExtraConfig storageExtraConfig = setup.getConfig(StorageExtraConfig::builder);
        for (StorageFactoryCreator factoryCreator : loader) {
            val factories = factoryCreator.getStorageFactories();
            for (val factoryInfo : factories) {
                log.info("Loading {}, trying {}", storageImplementation, factoryInfo);
                if (factoryInfo.getName().equals(storageImplementation)
                        && factoryInfo.getStorageLayoutType() == storageLayoutType) {
                    StorageFactory factory = factoryCreator.createFactory(factoryInfo, setup, executor);
                    if (storageExtraConfig.isStorageNoOpMode()) {
                        //The specified storage implementation is in No-Op mode.
                        log.warn("{} IS IN NO-OP MODE: DATA LOSS WILL HAPPEN! MAKE SURE IT IS BY FULL INTENTION FOR TESTING PURPOSE!", storageImplementation);
                        StorageFactory storageFactory;
                        if (factory instanceof SimpleStorageFactory) {
                            storageFactory = new ConditionalNoOpStorageFactory(executor, (SimpleStorageFactory) factory, storageExtraConfig);
                        } else {
                            storageFactory = new NoOpStorageFactory(storageExtraConfig, executor, factory, null);
                        }
                        return storageFactory;
                    }
                    if (storageExtraConfig.isSlowModeEnabled()) {
                        log.warn("{} IS IN SLOW MODE: PERF DEGRADATION EXPECTED! MAKE SURE IT IS BY FULL INTENTION FOR TESTING PURPOSE!", storageImplementation);
                        return new SlowStorageFactory(executor, factory, storageExtraConfig);
                    }
                    return factory;
                }
            }
        }
        return null;
    }
}
