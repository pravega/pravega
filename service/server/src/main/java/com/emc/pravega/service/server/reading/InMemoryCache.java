package com.emc.pravega.service.server.reading;

import com.emc.pravega.service.storage.Cache;

import java.util.HashMap;

/**
 * In-Memory implementation of Cache.
 */
public class InMemoryCache implements Cache {
    private final HashMap<CacheKey, CacheEntry> map;

    public InMemoryCache(){
        this.map = new HashMap<>();
    }
    @Override
    public void insert(CacheEntry entry) {

    }

    @Override
    public CacheEntry get(CacheKey key) {
        return null;
    }

    @Override
    public boolean remove(CacheKey key) {
        return false;
    }
}
