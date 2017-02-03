/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.mocks;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.function.CallbackHelpers;
import com.emc.pravega.common.util.ByteArraySegment;
import com.emc.pravega.service.storage.Cache;
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
        return this.map.get(key);
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
