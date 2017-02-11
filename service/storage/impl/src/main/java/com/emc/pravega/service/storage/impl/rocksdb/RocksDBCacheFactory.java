/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.storage.impl.rocksdb;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.CacheFactory;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Cache Factory for RocksDB Cache implementation.
 */
@Slf4j
public class RocksDBCacheFactory implements CacheFactory {
    //region Members

    private static final String LOG_ID = "RocksDBCacheFactory";
    private final HashMap<String, RocksDBCache> caches;
    private final RocksDBConfig config;
    private final AtomicBoolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RocksDBCacheFactory class.
     *
     * @param config The configuration to use.
     */
    public RocksDBCacheFactory(RocksDBConfig config) {
        Preconditions.checkNotNull(config, "config");

        this.config = config;
        this.caches = new HashMap<>();
        this.closed = new AtomicBoolean();

        RocksDB.loadLibrary();
        log.info("{}: Initialized.", LOG_ID);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            ArrayList<RocksDBCache> toClose;
            synchronized (this.caches) {
                toClose = new ArrayList<>(this.caches.values());
            }

            toClose.forEach(RocksDBCache::close);
            this.closed.set(true);
            log.info("{}: Closed.", LOG_ID);
        }
    }

    //endregion

    //endregion

    //region CacheFactory Implementation

    @Override
    public Cache getCache(String id) {
        Exceptions.checkNotClosed(this.closed.get(), this);

        RocksDBCache result;
        boolean isNew = false;
        synchronized (this.caches) {
            result = this.caches.get(id);
            if (result == null) {
                result = new RocksDBCache(id, this.config, this::cacheClosed);
                this.caches.put(id, result);
                isNew = true;
            }
        }

        if (isNew) {
            result.initialize();
        }

        return result;
    }

    private void cacheClosed(String cacheId) {
        synchronized (this.caches) {
            this.caches.remove(cacheId);
        }
    }

    //endregion
}
