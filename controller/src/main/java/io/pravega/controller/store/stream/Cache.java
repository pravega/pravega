/**
 * Copyright Pravega Authors.
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
package io.pravega.controller.store.stream;

import com.google.common.cache.CacheBuilder;
import io.pravega.controller.store.VersionedMetadata;

import java.util.concurrent.TimeUnit;

/**
 * The cache for storing versioned records against a cache key.
 * Cache key is an interface and users of this cache and provide any cache key implementation. 
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
