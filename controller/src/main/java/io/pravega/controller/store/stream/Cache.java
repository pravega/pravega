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
import io.pravega.common.Exceptions;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

public class Cache {
    private static final int MAXIMUM_SIZE = 1000;

    @FunctionalInterface
    public interface Loader {
        CompletableFuture<Data> get(final String key);
    }

    private final LoadingCache<String, CompletableFuture<Data>> cache;

    public Cache(final Loader loader) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(MAXIMUM_SIZE)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(new CacheLoader<String, CompletableFuture<Data>>() {
                    @ParametersAreNonnullByDefault
                    @Override
                    public CompletableFuture<Data> load(final String key) {
                        return loader.get(key);
                    }
                });
    }

    public CompletableFuture<Data> getCachedData(final String key) {
        return cache.getUnchecked(key).exceptionally(ex -> {
            System.err.println("shivesh:: invalidating cache because of a previous exception.. key = " 
                    + key + " exception=" + Exceptions.unwrap(ex).getClass());
            invalidateCache(key);
            throw new CompletionException(ex);
        });
    }

    public void invalidateCache(final String key) {
        cache.invalidate(key);
    }

    public void invalidateAll() {
        cache.invalidateAll();
    }
}
