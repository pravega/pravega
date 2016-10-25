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

package com.emc.pravega.service.storage.impl.rocksdb;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.io.FileHelpers;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.CacheFactory;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

import java.io.File;
import java.nio.file.Paths;
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
    private static final String DB_LOG_DIR = "log";
    private static final String DB_WRITE_AHEAD_LOG_DIR = "wal";

    /**
     * Max RocksDB WAL Size MB.
     * See this for more info: https://github.com/facebook/rocksdb/wiki/Basic-Operations#purging-wal-files
     */
    private static final int MAX_WRITE_AHEAD_LOG_SIZE_MB = 64;
    private final HashMap<String, RocksDBCache> caches;
    private final RocksDBConfig config;
    private final AtomicBoolean closed;
    private Options options;

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
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            if (this.options != null) {
                this.options.close();
            }

            this.closed.set(true);

            ArrayList<RocksDBCache> toClose;
            synchronized (this.caches) {
                toClose = new ArrayList<>(this.caches.values());
            }

            toClose.forEach(RocksDBCache::close);
            log.info("{}: Closed.", LOG_ID);
        }
    }

    //endregion

    //region Initialization

    /**
     * Ensures all the file system is properly set up and loads the RocksDB library in memory.
     *
     * @param reset If true, the entire on-disk cache is cleared prior to initialization; otherwise it is reused.
     */
    public void initialize(boolean reset) {
        if (reset) {
            // Delete all existing databases.
            clear();
        }

        // Create (or recreate the database dir).
        createDatabaseDir();

        // Load the RocksDB C++ library. Doing this more than once has no effect, so it's safe to put in the constructor.
        RocksDB.loadLibrary();
        this.options = createOptions();

        log.info("{}: Initialized.", LOG_ID);
    }

    private void createDatabaseDir() {
        File file = new File(this.config.getDatabaseDir());
        if (file.mkdirs()) {
            log.info("{}: Created path '{}'.", LOG_ID, this.config.getDatabaseDir());
        }
    }

    private void clear() {
        Preconditions.checkState(this.options == null, "Cannot clear all caches after initialization.");

        // Delete all files for this database.
        File dbDir = new File(config.getDatabaseDir());
        if (FileHelpers.deleteFileOrDirectory(dbDir)) {
            log.debug("{}: Deleted database dir '%s'.", dbDir.getAbsolutePath());
        }
    }

    private Options createOptions() {
        return new Options()
                .setCreateIfMissing(true)
                .setDbLogDir(Paths.get(this.config.getDatabaseDir(), DB_LOG_DIR).toString())
                .setWalDir(Paths.get(this.config.getDatabaseDir(), DB_WRITE_AHEAD_LOG_DIR).toString())
                .setWalTtlSeconds(0)
                .setWalSizeLimitMB(MAX_WRITE_AHEAD_LOG_SIZE_MB);
    }

    //endregion

    //region CacheFactory Implementation

    @Override
    public Cache getCache(String id) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.options != null, "RocksDBCacheFactory not initialized.");

        synchronized (this.caches) {
            RocksDBCache result = this.caches.get(id);
            if (result == null) {
                result = new RocksDBCache(id, this.options, this.config);
                result.setCloseCallback(idToRemove -> {
                    synchronized (this.caches) {
                        this.caches.remove(idToRemove);
                    }
                });
                this.caches.put(id, result);
            }

            return result;
        }
    }

    //endregion
}
