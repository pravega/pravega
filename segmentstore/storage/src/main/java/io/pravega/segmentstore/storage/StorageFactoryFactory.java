/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import io.pravega.common.ConfigSetup;
import java.util.concurrent.ScheduledExecutorService;

public interface StorageFactoryFactory {
    /**
     * API to create a storage factory with given configuration.
     * @param setup     Configuration for the factory.
     * @param executor  The storage factory is expected to use this ExecutorService for execution of its tasks.
     */
    StorageFactory createFactory(ConfigSetup setup, ScheduledExecutorService executor);

    /**
     * The unique name for the storage factory.
     * @return  Unique name for the storage factory.
     */
    String getName();
}
