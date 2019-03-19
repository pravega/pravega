/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Cache for asynchronously retrieving and loading records from underlying store using the supplied loader.
 * This uses Guava's loading cache and takes a loader function for loading entries into the cache. 
 * This class caches Futures which hold the metadata record with version. The cache is untyped and the CompletableFutures
 * can hold any value under the VersionedMetadata wrapper.
 * The values are by default held for 2 minutes after creation unless invalidated explicitly.
 * The maximum number of records that can be held in the cache is 10000. 
 */
public class Cache {
    private static final int MAX_CACHE_SIZE = 10000;
    
    private final LoadingCache<CacheKey, CompletableFuture<VersionedMetadata<?>>> cache;

    public Cache(final Function<CacheKey, CompletableFuture<VersionedMetadata<?>>> loader) {
        cache = CacheBuilder.newBuilder()
                            .maximumSize(MAX_CACHE_SIZE)
                            .expireAfterWrite(2, TimeUnit.MINUTES)
                            .build(new CacheLoader<CacheKey, CompletableFuture<VersionedMetadata<?>>>() {
                    @ParametersAreNonnullByDefault
                    @Override
                    public CompletableFuture<VersionedMetadata<?>> load(final CacheKey key) {
                        CompletableFuture<VersionedMetadata<?>> result = loader.apply(key);
                        result.exceptionally(ex -> {
                            invalidateCache(key);
                            return null;
                        });
                        return result;
                    }
                });
    }

    CompletableFuture<VersionedMetadata<?>> getCachedData(CacheKey key) {
        return cache.getUnchecked(key);
    }

    void invalidateCache(final CacheKey key) {
        cache.invalidate(key);
    }

    /**
     * All entries in the cache are cached against objects of type CacheKey. 
     */
    public interface CacheKey {
    }
}
