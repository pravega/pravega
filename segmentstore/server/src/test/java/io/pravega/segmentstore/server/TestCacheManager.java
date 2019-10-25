/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Exposes the applyCachePolicy method in the CacheManager.
 */
public class TestCacheManager extends CacheManager {
    public TestCacheManager(CachePolicy policy, ScheduledExecutorService executorService) {
        super(policy, executorService);
    }

    @Override
    public boolean applyCachePolicy() {
        return super.applyCachePolicy();
    }
}