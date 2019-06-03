/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import io.pravega.common.Exceptions;
import io.pravega.common.function.Callbacks;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.Cache;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;

/**
 * In-Memory implementation of Cache.
 */
@ThreadSafe
public class InMemoryCache implements Cache {
    private final Map<Key, byte[]> map;
    private final String id;
    private final Consumer<String> closeCallback;
    private final AtomicBoolean closed;

    /**
     * Creates a new instance of the InMemoryCache class.
     *
     * @param id The cache Id.
     */
    public InMemoryCache(String id) {
        this(id, null);
    }

    /**
     * Creates a new instance of the InMemoryCache class.
     *
     * @param id            The cache Id.
     * @param closeCallback A callback to invoke when the Cache is closed.
     */
    InMemoryCache(String id, Consumer<String> closeCallback) {
        this.id = id;
        this.map = new ConcurrentHashMap<>();
        this.closeCallback = closeCallback;
        this.closed = new AtomicBoolean();
    }

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (this.closed.compareAndSet(false, true)) {
            // Clean up the map, just in case the caller still has a pointer to this object.
            this.map.clear();

            Consumer<String> callback = this.closeCallback;
            if (callback != null) {
                Callbacks.invokeSafely(callback, this.id, null);
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
        Exceptions.checkNotClosed(this.closed.get(), this);
        this.map.put(key, payload);
    }

    @Override
    public void insert(Cache.Key key, ByteArraySegment data) {
        insert(key, data.getCopy());
    }

    @Override
    public byte[] get(Cache.Key key) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        // Make sure we return a copy of the data; if we return a pointer to the array, then someone can simply modify
        // the data in the cache, which is not allowed.
        byte[] data = this.map.get(key);
        return data == null ? null : Arrays.copyOf(data, data.length);
    }

    @Override
    public void remove(Cache.Key key) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        this.map.remove(key);
    }

    //endregion

    public void clear() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        this.map.clear();
    }
}
