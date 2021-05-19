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

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import io.pravega.controller.store.VersionedMetadata;
import lombok.Data;

import java.util.concurrent.TimeUnit;

/**
 * The cache for storing versioned records against a cache key.
 * Cache key is an interface and users of this cache and provide any cache key implementation.
 * This cache has a logic to update a value into the cache if the new value is newer (higher version number)
 * than the existing value. 
 * This is achieved by doing a conditional update of cached values. 
 */
public class Cache {
    private static final int MAX_CACHE_SIZE = 10000;
    private static final int KEY_PARTITION_COUNT = 100;

    private final com.google.common.cache.Cache<CacheKey, CacheValue> cache;
    // The cache uses fixed number of locks used for conditional updates of keys. 
    // We use a thread safe implementation of cache. However, cache does not provide conditional update for its keys
    // based on key version. So if the cache has an existing value which is older than new value, then its updated, 
    // else ignored. This ensures we do not overwrite a newer entry in cache with an older entry.   
    // To achieve conditional update, we divide the entire key space into logical partitions. So each key is hashed and
    // mapped to one logical partition. 
    // For conditional update to a key, we first acquire lock on the key partition, and then we do a get and update 
    // on that key subject to the time condition. 
    // By partitioning the cache, we actually increase the concurrency by not relying on a single lock to protect this 
    // cache class. 
    private final Object[] locks = new Object[KEY_PARTITION_COUNT];
    public Cache() {
        cache = CacheBuilder.newBuilder()
                            .maximumSize(MAX_CACHE_SIZE)
                            .expireAfterAccess(2, TimeUnit.MINUTES)
                            .build();
        for (int i = 0; i < KEY_PARTITION_COUNT; i++) {
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
        synchronized (getLockObject(key)) {
            cache.invalidate(key);
        }
    }
    
    public void put(CacheKey cacheKey, VersionedMetadata<?> record, long time) {
        Preconditions.checkNotNull(record, "Null record cannot be put in cache");
        // acquire the lock for key partition. Then perform a conditional update - get, compare and swap.
        // condition for update => if either the entry doesnt exist in cache. OR the entry in cache is older (lower key version)
        // than new value to be updated. 
        synchronized (getLockObject(cacheKey)) {
            Cache.CacheValue existing = cache.getIfPresent(cacheKey);
            if (existing == null || record.getVersion().compareTo(existing.getValue().getVersion()) > 0) {
                cache.put(cacheKey, new CacheValue(record, time));
            }
        }
    }

    private Object getLockObject(CacheKey key) {
        int lockIndex = Math.abs(key.hashCode() % KEY_PARTITION_COUNT);
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
