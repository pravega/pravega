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
import com.emc.pravega.common.io.FileHelpers;
import com.emc.pravega.common.util.ByteArraySegment;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.CacheException;
import com.google.common.base.Preconditions;
import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;

/**
 * RocksDB-backed Cache.
 */
@Slf4j
class RocksDBCache implements Cache {
    //region Members

    private static final String FILE_PREFIX = "cache_";
    private static final String DB_LOG_DIR = "log";
    private static final String DB_WRITE_AHEAD_LOG_DIR = "wal";

    /**
     * Max RocksDB WAL Size MB.
     * See this for more info: https://github.com/facebook/rocksdb/wiki/Basic-Operations#purging-wal-files
     */
    private static final int MAX_WRITE_AHEAD_LOG_SIZE_MB = 64;

    @Getter
    private final String id;
    private final Options databaseOptions;
    private final WriteOptions writeOptions;
    private final AtomicReference<RocksDB> database;
    private final AtomicBoolean closed;
    private final String dbDir;
    private final String logId;
    private final Consumer<String> closeCallback;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RocksDBCache class.
     *
     * @param id            The Cache Id.
     * @param config        RocksDB configuration.
     * @param closeCallback A callback to invoke when the cache is closed.
     */
    RocksDBCache(String id, RocksDBConfig config, Consumer<String> closeCallback) {
        Exceptions.checkNotNullOrEmpty(id, "id");
        Preconditions.checkNotNull(config, "config");

        this.id = id;
        this.logId = String.format("RocksDBCache[%s]", id);
        this.dbDir = Paths.get(config.getDatabaseDir(), FILE_PREFIX + this.id).toString();
        this.closeCallback = closeCallback;
        this.closed = new AtomicBoolean();
        this.database = new AtomicReference<>();
        try {
            this.databaseOptions = createDatabaseOptions();
            this.writeOptions = createWriteOptions();
        } catch (Exception ex) {
            // Make sure we cleanup anything we may have created in case of failure.
            try {
                close();
            } catch (Exception closeEx) {
                ex.addSuppressed(closeEx);
            }

            throw ex;
        }
    }

    /**
     * Initializes this instance of the RocksDB cache. This method must be invoked before the cache can be used.
     */
    void initialize() {
        Preconditions.checkState(this.database.get() == null, "%s has already been initialized.", this.logId);

        try {
            clear(true);
            this.database.set(openDatabase());
        } catch (Exception ex) {
            // Make sure we cleanup anything we may have created in case of failure.
            try {
                close();
            } catch (Exception closeEx) {
                ex.addSuppressed(closeEx);
            }

            throw ex;
        }

        log.info("{}: Initialized.", this.logId);
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        if (!this.closed.get()) {
            RocksDB db = this.database.get();
            if (db != null) {
                db.close();
                this.database.set(null);
            }

            if (this.writeOptions != null) {
                this.writeOptions.close();
            }

            if (this.databaseOptions != null) {
                this.databaseOptions.close();
            }

            clear(false);
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
    public void insert(Key key, byte[] data) {
        ensureInitializedAndNotClosed();
        try {
            this.database.get().put(this.writeOptions, key.getSerialization(), data);
        } catch (RocksDBException ex) {
            throw convert(ex, "insert key '%s'", key);
        }
    }

    @Override
    public void insert(Key key, ByteArraySegment data) {
        insert(key, data.getCopy());
    }

    @Override
    public byte[] get(Key key) {
        ensureInitializedAndNotClosed();
        try {
            return this.database.get().get(key.getSerialization());
        } catch (RocksDBException ex) {
            throw convert(ex, "get key '%s'", key);
        }
    }

    @Override
    public void remove(Key key) {
        ensureInitializedAndNotClosed();
        try {
            this.database.get().remove(this.writeOptions, key.getSerialization());
        } catch (RocksDBException ex) {
            throw convert(ex, "remove key '%s'", key);
        }
    }

    //endregion

    //region Helpers

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
            return RocksDB.open(this.databaseOptions, this.dbDir);
        } catch (RocksDBException ex) {
            throw convert(ex, "initialize RocksDB instance");
        }
    }

    private Options createDatabaseOptions() {
        return new Options()
                .setCreateIfMissing(true)
                .setDbLogDir(Paths.get(this.dbDir, DB_LOG_DIR).toString())
                .setWalDir(Paths.get(this.dbDir, DB_WRITE_AHEAD_LOG_DIR).toString())
                .setWalTtlSeconds(0)
                .setWalSizeLimitMB(MAX_WRITE_AHEAD_LOG_SIZE_MB);
    }

    private void clear(boolean recreateDirectory) {
        File dbDir = new File(this.dbDir);
        if (FileHelpers.deleteFileOrDirectory(dbDir)) {
            log.debug("{}: Deleted existing database directory '{}'.", this.logId, dbDir.getAbsolutePath());
        }

        if (recreateDirectory && dbDir.mkdirs()) {
            log.info("{}: Created empty database directory '{}'.", this.logId, dbDir.getAbsolutePath());
        }
    }

    private RuntimeException convert(RocksDBException exception, String message, Object... messageFormatArgs) {
        String exceptionMessage = String.format(
                "Unable to %s (CacheId=%s).",
                String.format(message, messageFormatArgs),
                this.id);

        throw new CacheException(exceptionMessage, exception);
    }

    private void ensureInitializedAndNotClosed() {
        Exceptions.checkNotClosed(this.closed.get(), this);
        Preconditions.checkState(this.database.get() != null, "%s has not been initialized.", this.logId);
    }

    //endregion
}
