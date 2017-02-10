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
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Factory for InMemoryCache.
 */
@ThreadSafe
public class InMemoryCacheFactory implements CacheFactory {
    @GuardedBy("caches")
    private final HashMap<String, InMemoryCache> caches = new HashMap<>();
    @GuardedBy("caches")
    private boolean closed;

    @Override
    public Cache getCache(String id) {
        synchronized (this.caches) {
            Exceptions.checkNotClosed(this.closed, this);
            return this.caches.computeIfAbsent(id, key -> new InMemoryCache(key, this::cacheClosed));
        }
    }

    @Override
    public void close() {
        ArrayList<InMemoryCache> toClose = null;
        synchronized (this.caches) {
            if (!this.closed) {
                this.closed = true;
                toClose = new ArrayList<>(this.caches.values());
            }
        }

        if (toClose != null) {
            toClose.forEach(InMemoryCache::close);
        }
    }

    private void cacheClosed(String cacheId) {
        synchronized (this.caches) {
            this.caches.remove(cacheId);
        }
    }
}
