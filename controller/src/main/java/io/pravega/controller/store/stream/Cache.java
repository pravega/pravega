/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.cache.CacheBuilder;
import io.pravega.controller.store.VersionedMetadata;
import lombok.Data;

import java.util.concurrent.TimeUnit;

/**
 * The cache for storing versioned records against a cache key.
 * Cache key is an interface and users of this cache and provide any cache key implementation. 
 */
public class Cache {
    private static final int MAX_CACHE_SIZE = 10000;
    
    private final com.google.common.cache.Cache<CacheKey, CacheValue> cache;
    private final Object[] locks = new Object[100];
    public Cache() {
        cache = CacheBuilder.newBuilder()
                            .maximumSize(MAX_CACHE_SIZE)
                            .expireAfterAccess(2, TimeUnit.MINUTES)
                            .build();
        for (int i = 0; i < 100; i++) {
            locks[i] = new Object();
        }
    }

    public VersionedMetadata<?> getCachedData(CacheKey key) {
        Cache.CacheValue value = cache.getIfPresent(key);
        if (value != null) {
            return value.getValue();
        } else {
            return null;
        }
    }

    public VersionedMetadata<?> getCachedData(CacheKey key, long time) {
        Cache.CacheValue value = cache.getIfPresent(key);
        if (value != null && value.getTime() > time) {
            return value.getValue();
        } else {
            return null;
        }
    }

    public void invalidateCache(final CacheKey key) {
        cache.invalidate(key);
    }

    public void put(CacheKey cacheKey, VersionedMetadata<?> record, long time) {
        synchronized (getLock(cacheKey)) {
            Cache.CacheValue existing = cache.getIfPresent(cacheKey);
            if (existing == null || record.getVersion().compareTo(existing.getValue().getVersion()) > 0) {
                cache.put(cacheKey, new CacheValue(record, time));
            }
        }
    }

    private Object getLock(CacheKey key) {
        int lockIndex = Math.abs(key.hashCode() % 100);
        return locks[lockIndex];
    }

    /**
     * All entries in the cache are cached against objects of type CacheKey. 
     */
    public interface CacheKey {
    }
    
    @Data
    static class CacheValue {
        private final VersionedMetadata<?> value;
        private final long time;
    }
}
