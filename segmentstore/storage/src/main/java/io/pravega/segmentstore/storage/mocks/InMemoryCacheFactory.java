/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import io.pravega.common.Exceptions;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.CacheFactory;

import java.util.ArrayList;
import java.util.HashMap;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for InMemoryCache.
 */
@ThreadSafe
public class InMemoryCacheFactory implements CacheFactory {
    @GuardedBy("caches")
    private final HashMap<String, InMemoryCache> caches = new HashMap<>();
    @GuardedBy("caches")
    private boolean closed;

    @Override
    public Cache getCache(String id) {
        synchronized (this.caches) {
            Exceptions.checkNotClosed(this.closed, this);
            return this.caches.computeIfAbsent(id, key -> new InMemoryCache(key, this::cacheClosed));
        }
    }

    @Override
    public void close() {
        ArrayList<InMemoryCache> toClose = null;
        synchronized (this.caches) {
            if (!this.closed) {
                this.closed = true;
                toClose = new ArrayList<>(this.caches.values());
            }
        }

        if (toClose != null) {
            toClose.forEach(InMemoryCache::close);
        }
    }

    private void cacheClosed(String cacheId) {
        synchronized (this.caches) {
            this.caches.remove(cacheId);
        }
    }
}
