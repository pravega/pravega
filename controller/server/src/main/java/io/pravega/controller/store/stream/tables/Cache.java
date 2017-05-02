/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.controller.store.stream.tables;

import io.pravega.controller.store.stream.DataNotFoundException;
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
                .maximumSize(10000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(new CacheLoader<String, CompletableFuture<Data<T>>>() {
                    @ParametersAreNonnullByDefault
                    @Override
                    public CompletableFuture<Data<T>> load(final String key) {
                        CompletableFuture<Data<T>> result = loader.get(key);
                        result.exceptionally(ex -> {
                            invalidateCache(key);
                            return null;
                        });
                        return result;
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
}
