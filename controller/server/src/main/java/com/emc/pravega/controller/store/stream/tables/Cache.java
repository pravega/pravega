/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream.tables;

import com.emc.pravega.controller.store.stream.DataNotFoundException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class Cache<T> {

    @FunctionalInterface
    public interface Loader<U> {
        CompletableFuture<Data<U>> get(final String key) throws DataNotFoundException;
    }

    private final LoadingCache<String, CompletableFuture<Data<T>>> cache;

    public Cache(final Loader<T> loader) {
        cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, CompletableFuture<Data<T>>>() {
                            @ParametersAreNonnullByDefault
                            public CompletableFuture<Data<T>> load(final String key) {
                                try {
                                    return loader.get(key);
                                } catch (DataNotFoundException d) {
                                    throw d;
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
    }

    public CompletableFuture<Data<T>> getCachedData(final String key) {
        return cache.getUnchecked(key);
    }

    public Void invalidateCache(final String key) {
        cache.invalidate(key);
        return null;
    }

    public Void invalidateAll() {
        cache.invalidateAll();
        return null;
    }


    public Void invalidateAll(final Iterable<String> keys) {
        cache.invalidateAll(keys);
        return null;
    }
}
