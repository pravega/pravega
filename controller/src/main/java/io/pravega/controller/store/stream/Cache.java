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

public class Cache {
    
    private final LoadingCache<CacheKey, CompletableFuture<VersionedMetadata<?>>> cache;

    public Cache(final Function<CacheKey, CompletableFuture<VersionedMetadata<?>>> loader) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
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
    
    public interface CacheKey {
    }
}
