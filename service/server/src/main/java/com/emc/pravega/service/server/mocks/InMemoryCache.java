package com.emc.pravega.service.server.mocks;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.function.CallbackHelpers;
import com.emc.pravega.service.storage.Cache;

import java.util.HashMap;
import java.util.function.Consumer;

/**
 * In-Memory implementation of Cache.
 */
public class InMemoryCache implements Cache {
    private final HashMap<Cache.Key, byte[]> map;
    private final String id;
    private Consumer<String> closeCallback;
    private boolean closed;

    /**
     * Creates a new instance of the InMemoryCache class.
     */
    public InMemoryCache(String id) {
        this.id = id;
        this.map = new HashMap<>();
    }

    void setCloseCallback(Consumer<String> callback) {
        this.closeCallback = callback;
    }

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed) {
            this.closed = true;

            // Clean up the map, just in case the caller still has a pointer to this object.
            synchronized (this.map) {
                this.map.clear();
            }

            Consumer<String> callback = this.closeCallback;
            if (callback != null) {
                CallbackHelpers.invokeSafely(callback, this.id, null);
            }
        }
    }

    //endregion

    //region Cache Implementation

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public void insert(Cache.Key key, byte[] payload) {
        Exceptions.checkNotClosed(this.closed, this);
        synchronized (this.map) {
            this.map.put(key, payload);
            key.markInCache(true);
        }
    }

    @Override
    public byte[] get(Cache.Key key) {
        Exceptions.checkNotClosed(this.closed, this);
        synchronized (this.map) {
            return this.map.get(key);
        }
    }

    @Override
    public boolean remove(Cache.Key key) {
        Exceptions.checkNotClosed(this.closed, this);
        synchronized (this.map) {
            key.markInCache(false);
            return this.map.remove(key) != null;
        }
    }

    @Override
    public void reset() {
        Exceptions.checkNotClosed(this.closed, this);
        synchronized (this.map) {
            this.map.clear();
        }
    }

    //endregion
}
