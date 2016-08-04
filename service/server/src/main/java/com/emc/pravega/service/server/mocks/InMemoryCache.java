package com.emc.pravega.service.server.mocks;

import com.emc.pravega.service.server.reading.CacheKey;
import com.emc.pravega.service.storage.Cache;

import java.util.HashMap;

/**
 * In-Memory implementation of Cache.
 */
public class InMemoryCache implements Cache<CacheKey> {
    private final HashMap<CacheKey, byte[]> map;

    /**
     * Creates a new instance of the InMemoryCache class.
     */
    public InMemoryCache() {
        this.map = new HashMap<>();
    }

    //region Cache Implementation

    @Override
    public void insert(CacheKey key, byte[] payload) {
        synchronized (this.map) {
            this.map.put(key, payload);
        }
    }

    @Override
    public byte[] get(CacheKey key) {
        synchronized (this.map) {
            return this.map.get(key);
        }
    }

    @Override
    public boolean remove(CacheKey key) {
        synchronized (this.map) {
            return this.map.remove(key) != null;
        }
    }

    //endregion
}
