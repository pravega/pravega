/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import io.pravega.segmentstore.storage.datastore.DataStore;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Exposes the applyCachePolicy method in the CacheManager.
 */
public class TestCacheManager extends CacheManager {
    public TestCacheManager(CachePolicy policy, ScheduledExecutorService executorService) {
        super(policy, executorService);
    }

    public TestCacheManager(CachePolicy policy, DataStore dataStore, ScheduledExecutorService executorService) {
        super(policy, dataStore, executorService);
    }

    @Override
    public void applyCachePolicy() {
        super.applyCachePolicy();
    }
}