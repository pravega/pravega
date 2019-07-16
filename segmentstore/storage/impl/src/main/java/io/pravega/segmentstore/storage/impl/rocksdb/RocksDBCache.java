/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.rocksdb;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.Timer;
import io.pravega.common.function.Callbacks;
import io.pravega.common.io.FileHelpers;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.CacheException;
import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Logger;
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

    /**
     * Max number of in-memory write buffers (memtables) for the cache (active and immutable).
     */
    private static final int MAX_WRITE_BUFFER_NUMBER = 4;

    /**
     * Minimum number of in-memory write buffers (memtables) to be merged before flushing to storage.
     */
    private static final int MIN_WRITE_BUFFER_NUMBER_TO_MERGE = 2;

    @Getter
    private final String id;
    private final Options databaseOptions;
    private final WriteOptions writeOptions;
    private final AtomicReference<RocksDB> database;
    private final AtomicBoolean closed;
    private final String dbDir;
    private final String logId;
    private final Consumer<String> closeCallback;
    private final int writeBufferSizeMB;
    private final int readCacheSizeMB;
    private final int cacheBlockSizeKB;
    private final String rocksDBLogLevel;

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
        // The total write buffer space is divided into the number of buffers.
        this.writeBufferSizeMB = config.getWriteBufferSizeMB() / MAX_WRITE_BUFFER_NUMBER;
        this.readCacheSizeMB = config.getReadCacheSizeMB();
        this.cacheBlockSizeKB = config.getCacheBlockSizeKB();
        this.rocksDBLogLevel = config.getRocksDBLogLevel();
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
        if (this.closed.compareAndSet(false, true)) {
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

            Consumer<String> callback = this.closeCallback;
            if (callback != null) {
                Callbacks.invokeSafely(callback, this.id, null);
            }
        }
    }

    //endregion

    //region Cache Implementation

    @Override
    public void insert(Key key, byte[] data) {
        ensureInitializedAndNotClosed();
        Timer timer = new Timer();
        byte[] serializedKey = key.serialize();
        try {
            this.database.get().put(this.writeOptions, serializedKey, data);
        } catch (RocksDBException ex) {
            throw convert(ex, "insert key '%s'", key);
        }

        RocksDBMetrics.insert(timer.getElapsedMillis(), serializedKey.length + ((data != null) ? data.length : 0));
    }

    @Override
    public void insert(Key key, ByteArraySegment data) {
        insert(key, data.getCopy());
    }

    @Override
    public byte[] get(Key key) {
        ensureInitializedAndNotClosed();
        Timer timer = new Timer();
        byte[] result;
        byte[] serializedKey = key.serialize();
        try {
            result = this.database.get().get(serializedKey);
        } catch (RocksDBException ex) {
            throw convert(ex, "get key '%s'", key);
        }

        RocksDBMetrics.get(timer.getElapsedMillis(), (result != null) ? result.length : 0);
        return result;
    }

    @Override
    public void remove(Key key) {
        ensureInitializedAndNotClosed();
        try {
            this.database.get().delete(this.writeOptions, key.serialize());
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
        BlockBasedTableConfig tableFormatConfig = new BlockBasedTableConfig()
                .setBlockSize(cacheBlockSizeKB * 1024L)
                .setBlockCacheSize(readCacheSizeMB * 1024L * 1024L)
                .setCacheIndexAndFilterBlocks(true);

        Options databaseOptions = new Options()
                .setCreateIfMissing(true)
                .setDbLogDir(Paths.get(this.dbDir, DB_LOG_DIR).toString())
                .setWalDir(Paths.get(this.dbDir, DB_WRITE_AHEAD_LOG_DIR).toString())
                .setWalTtlSeconds(0)
                .setWalSizeLimitMB(MAX_WRITE_AHEAD_LOG_SIZE_MB)
                .setWriteBufferSize(writeBufferSizeMB * 1024L * 1024L)
                .setMaxWriteBufferNumber(MAX_WRITE_BUFFER_NUMBER)
                .setMinWriteBufferNumberToMerge(MIN_WRITE_BUFFER_NUMBER_TO_MERGE)
                .setTableFormatConfig(tableFormatConfig)
                .setOptimizeFiltersForHits(true)
                .setUseDirectReads(true);

        InfoLogLevel logLevel = translateRocksDBLogLevel(rocksDBLogLevel);
        Options logOptions = new Options().
                setInfoLogLevel(logLevel).
                setCreateIfMissing(true);

        Logger logger = new Logger(logOptions) {
            @Override
            protected void log(InfoLogLevel infoLogLevel, String logMsg) {
                switch (infoLogLevel) {
                    case DEBUG_LEVEL:
                        log.debug(logMsg);
                        break;
                    case INFO_LEVEL:
                        log.info(logMsg);
                        break;
                    case WARN_LEVEL:
                        log.warn(logMsg);
                        break;
                    case ERROR_LEVEL:
                        log.error(logMsg);
                        break;
                    default:
                        log.error(logMsg);
                        break;
                }
            }
        };
        databaseOptions.setLogger(logger);
        return databaseOptions;
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

    private InfoLogLevel translateRocksDBLogLevel(String logLevel) {
        InfoLogLevel rocksDBlogLevel;
        switch (logLevel) {
            case "DEBUG":
                rocksDBlogLevel = InfoLogLevel.DEBUG_LEVEL;
                break;
            case "INFO":
                rocksDBlogLevel = InfoLogLevel.INFO_LEVEL;
                break;
            case "WARN":
                rocksDBlogLevel = InfoLogLevel.WARN_LEVEL;
                break;
            case "ERROR":
                rocksDBlogLevel = InfoLogLevel.ERROR_LEVEL;
                break;
            case "FATAL":
                rocksDBlogLevel = InfoLogLevel.FATAL_LEVEL;
                break;
            case "HEADER":
                rocksDBlogLevel = InfoLogLevel.HEADER_LEVEL;
                break;
            default:
                rocksDBlogLevel = InfoLogLevel.ERROR_LEVEL;
                break;
        }
        return rocksDBlogLevel;
    }

    //endregion
}
