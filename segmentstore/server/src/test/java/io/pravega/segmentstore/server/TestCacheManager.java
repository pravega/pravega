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

import io.pravega.segmentstore.storage.cache.CacheStorage;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import lombok.Setter;

/**
 * Exposes the applyCachePolicy method in the CacheManager.
 */
public class TestCacheManager extends CacheManager {
    @Setter
    private Consumer<Client> unregisterInterceptor;

    public TestCacheManager(CachePolicy policy, CacheStorage cacheStorage, ScheduledExecutorService executorService) {
        super(policy, cacheStorage, executorService);
    }

    @Override
    public boolean applyCachePolicy() {
        return super.applyCachePolicy();
    }

    @Override
    public void unregister(Client client) {
        Consumer<Client> interceptor = this.unregisterInterceptor;
        if (interceptor != null) {
            interceptor.accept(client);
        }

        super.unregister(client);
    }
}