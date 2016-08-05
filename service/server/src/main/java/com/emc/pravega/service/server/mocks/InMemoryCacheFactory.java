package com.emc.pravega.service.server.mocks;

import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.CacheFactory;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Factory for InMemoryCache.
 */
public class InMemoryCacheFactory implements CacheFactory {
    private final HashMap<String, InMemoryCache> caches = new HashMap<>();
    private boolean closed;

    @Override
    public Cache getCache(String id) {
        InMemoryCache result;
        synchronized (this.caches) {
            result = this.caches.get(id);
            if (result == null) {
                result = new InMemoryCache(id);
                result.setCloseCallback(idToRemove -> {
                    synchronized (this.caches) {
                        this.caches.remove(idToRemove);
                    }
                });
                this.caches.put(id, result);
            }
        }

        return result;
    }

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;
            ArrayList<InMemoryCache> toClose;
            synchronized (this.caches) {
                toClose = new ArrayList<>(this.caches.values());
            }

            toClose.forEach(InMemoryCache::close);
            assert this.caches.size() == 0 : "not all caches were closed";
        }
    }
}
