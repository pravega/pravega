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
import com.emc.pravega.common.function.CallbackHelpers;
import com.emc.pravega.common.util.ByteArraySegment;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.CacheException;
import com.emc.pravega.service.storage.CacheNotAvailableException;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * RocksDB-backed Cache.
 */
@Slf4j
class RocksDBCache implements Cache {
    //region Members

    private static final String FILE_PREFIX = "cache_";

    private final String id;
    private final RocksDBConfig config;
    private final Options databaseOptions;
    private final WriteOptions writeOptions;
    private final AtomicReference<RocksDB> database;
    private final AtomicBoolean closed;
    private final String logId;
    private Consumer<String> closeCallback;

    //endregion

    //region Constructor

    RocksDBCache(String id, Options databaseOptions, RocksDBConfig config) {
        Exceptions.checkNotNullOrEmpty(id, "id");
        Preconditions.checkNotNull(databaseOptions, "databaseOptions");
        Preconditions.checkNotNull(config, "config");

        this.id = id;
        this.logId = String.format("RocksDBCache[%s]", id);
        this.databaseOptions = databaseOptions;
        this.config = config;
        this.closed = new AtomicBoolean();
        this.database = new AtomicReference<>();
        try {
            this.database.set(openDatabase());
            this.writeOptions = createWriteOptions();
        } catch (Exception ex) {
            // Make sure we cleanup anything we may have created in case of failure.
            close();
            throw ex;
        }

        log.info("{}: Created.", this.logId);
    }

    /**
     * Attaches a callback to be invoked when the Cache is closed.
     *
     * @param callback The callback to attach.
     */
    void setCloseCallback(Consumer<String> callback) {
        this.closeCallback = callback;
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            RocksDB db = this.database.get();
            if (db != null) {
                db.close();
            }

            if (this.writeOptions != null) {
                this.writeOptions.close();
            }

            log.info("{}: Closed.", this.logId);
            this.closed.set(true);

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
    public void insert(Key key, byte[] data) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        try {
            getDatabase().put(this.writeOptions, key.getSerialization(), data);
        } catch (RocksDBException ex) {
            throw convert(ex, "insert key '%s'", key);
        }
    }

    @Override
    public void insert(Key key, ByteArraySegment data) {
        byte[] buffer = new byte[data.getLength()];
        data.copyTo(buffer, 0, buffer.length);
        insert(key, buffer);
    }

    @Override
    public byte[] get(Key key) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        try {
            return getDatabase().get(key.getSerialization());
        } catch (RocksDBException ex) {
            throw convert(ex, "get key '%s'", key);
        }
    }

    @Override
    public boolean remove(Key key) {
        Exceptions.checkNotClosed(this.closed.get(), this);
        try {
            getDatabase().remove(this.writeOptions, key.getSerialization());
        } catch (RocksDBException ex) {
            throw convert(ex, "remove key '%s'", key);
        }

        // RocksDB.remove does not have any special code when the key does not exist (nor is it treated as an error).
        return true;
    }

    //endregion

    private RocksDB getDatabase() {
        RocksDB db = this.database.get();
        if (db == null) {
            throw new CacheNotAvailableException(String.format("Cache '%s' is unavailable.", this.id));
        }

        return db;
    }

    /**
     * Creates the RocksDB WriteOptions to use. Since we use RocksDB as an in-process cache with disk spillover,
     * we do not care about the data being persisted to disk for recovery purposes. As such:
     * * Write-Ahead-Log is disabled (2.8x performance improvement)
     * * Sync is disabled - does not wait for a disk flush before returning from the write call (50x or more improvement).
     */
    private WriteOptions createWriteOptions() {
        return new WriteOptions()
                .setDisableWAL(true)
                .setSync(false);
    }

    private RocksDB openDatabase() {
        try {
            return RocksDB.open(this.databaseOptions, getDatabaseDir());
        } catch (RocksDBException ex) {
            throw convert(ex, "initialize RocksDB instance");
        }
    }

    private String getDatabaseDir() {
        return Paths.get(this.config.getDatabaseDir(), FILE_PREFIX + this.id).toString();
    }

    private RuntimeException convert(RocksDBException exception, String message, Object... messageFormatArgs) {
        String exceptionMessage = String.format(
                "Unable to %s (CacheId=%s).",
                String.format(message, messageFormatArgs),
                this.id);

        throw new CacheException(exceptionMessage, exception);
    }
}
