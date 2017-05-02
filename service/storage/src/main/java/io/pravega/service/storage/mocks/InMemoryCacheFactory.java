/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.service.storage.mocks;

import io.pravega.common.Exceptions;
import io.pravega.service.storage.Cache;
import io.pravega.service.storage.CacheFactory;

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
