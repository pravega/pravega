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
            Exceptions.checkNotClosed(this.closed, this);
            result = this.caches.get(id);
            if (result == null) {
                result = new InMemoryCache(id, this::cacheClosed);
                this.caches.put(id, result);
            }
        }

        return result;
    }

    @Override
    public void close() {
        if (!this.closed) {
            ArrayList<InMemoryCache> toClose;
            synchronized (this.caches) {
                this.closed = true;
                toClose = new ArrayList<>(this.caches.values());
            }

            toClose.forEach(InMemoryCache::close);
        }
    }

    private void cacheClosed(String cacheId) {
        synchronized (this.caches) {
            this.caches.remove(cacheId);
        }
    }
}
