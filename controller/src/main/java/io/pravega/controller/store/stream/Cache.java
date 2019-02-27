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
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class Cache<T> {
    private static final int MAXIMUM_SIZE = 10000;
    
    private final LoadingCache<T, CompletableFuture<Data>> cache;

    public Cache(final Function<T, CompletableFuture<Data>> loader) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(MAXIMUM_SIZE)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(new CacheLoader<T, CompletableFuture<Data>>() {
                    @ParametersAreNonnullByDefault
                    @Override
                    public CompletableFuture<Data> load(final T key) {
                        return loader.apply(key);
                    }
                });
    }

    public CompletableFuture<Data> getCachedData(final T key) {
        return cache.getUnchecked(key).exceptionally(ex -> {
            invalidateCache(key);
            throw new CompletionException(ex);
        });
    }

    public void invalidateCache(final T key) {
        cache.invalidate(key);
    }
}
