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

import java.util.concurrent.TimeUnit;

/**
 * Cache for asynchronously retrieving and loading records from underlying store using the supplied loader.
 * This uses Guava's loading cache and takes a loader function for loading entries into the cache. 
 * This class caches Futures which hold the metadata record with version. The cache  
 * can hold any value under the VersionedMetadata wrapper.
 * The values are by default held for 2 minutes after creation unless invalidated explicitly.
 * The maximum number of records that can be held in the cache is 10000. 
 */
public class Cache {
    private static final int MAX_CACHE_SIZE = 10000;
    
    private final com.google.common.cache.Cache<CacheKey, VersionedMetadata<?>> cache;

    public Cache() {
        cache = CacheBuilder.newBuilder()
                            .maximumSize(MAX_CACHE_SIZE)
                            .expireAfterAccess(2, TimeUnit.MINUTES)
                            .build();
    }

    public VersionedMetadata<?> getCachedData(CacheKey key) {
        return cache.getIfPresent(key);
    }

    public void invalidateCache(final CacheKey key) {
        cache.invalidate(key);
    }

    public void put(CacheKey cacheKey, VersionedMetadata<?> record) {
        cache.put(cacheKey, record);
    }

    /**
     * All entries in the cache are cached against objects of type CacheKey. 
     */
    public interface CacheKey {
    }
}
