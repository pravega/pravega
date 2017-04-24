/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.reading;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Exposes the applyCachePolicy method in the CacheManager.
 */
class TestCacheManager extends CacheManager {
    TestCacheManager(CachePolicy policy, ScheduledExecutorService executorService) {
        super(policy, executorService);
    }

    @Override
    public void applyCachePolicy() {
        super.applyCachePolicy();
    }
}