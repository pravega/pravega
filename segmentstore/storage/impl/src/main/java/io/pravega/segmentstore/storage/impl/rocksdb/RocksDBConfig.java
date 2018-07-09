/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.impl.rocksdb;

import io.pravega.common.util.ConfigBuilder;
import io.pravega.common.util.ConfigurationException;
import io.pravega.common.util.Property;
import io.pravega.common.util.TypedProperties;
import lombok.Getter;

/**
 * Configuration for RocksDB-backed cache.
 */
public class RocksDBConfig {
    //region Config Names

    public static final Property<String> DATABASE_DIR = Property.named("dbDir", "/tmp/pravega/cache");
    public static final Property<Integer> WRITE_BUFFER_SIZE_MB = Property.named("writeBufferSizeMB", 64);
    public static final Property<Integer> READ_CACHE_SIZE_MB = Property.named("readCacheSizeMB", 64);
    public static final Property<Integer> CACHE_BLOCK_SIZE_KB = Property.named("cacheBlockSizeKB", 4);
    private static final String COMPONENT_CODE = "rocksdb";

    //endregion

    //region Members

    /**
     * The path to the RocksDB database (in the local filesystem).
     */
    @Getter
    private final String databaseDir;

    /**
     * RocksDB allows to buffer writes in-memory (memtables) to improve write performance, thus executing an async flush
     * process of writes to disk. This parameter bounds the maximum amount of memory devoted to absorb writes.
     */
    @Getter
    private final Integer writeBufferSizeMB;

    /**
     * RocksDB caches (uncompressed) data blocks in memory to serve read requests with high performance in case of a
     * cache hit. This parameter bounds the maximum amount of memory devoted to cache uncompressed data blocks.
     */
    @Getter
    private final Integer readCacheSizeMB;

    /**
     * RocksDB stores data in memory related to internal indexes (e.g., it may range between 5% to 30% of the total
     * memory consumption depending on the configuration and data at hand). The size of the internal indexes in RocksDB
     * mainly depend on the size of cached data blocks (cacheBlockSizeKB). If you increase cacheBlockSizeKB, the number
     * of blocks will decrease, so the index size will also reduce linearly (but increasing read amplification).
     */
    @Getter
    private final Integer cacheBlockSizeKB;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the RocksDBConfig class.
     *
     * @param properties    The TypedProperties object to read Properties from.
     */
    private RocksDBConfig(TypedProperties properties) throws ConfigurationException {
        this.databaseDir = properties.get(DATABASE_DIR);
        this.writeBufferSizeMB = properties.getInt(WRITE_BUFFER_SIZE_MB);
        this.readCacheSizeMB = properties.getInt(READ_CACHE_SIZE_MB);
        this.cacheBlockSizeKB = properties.getInt(CACHE_BLOCK_SIZE_KB);
    }

    /**
     * Creates a new ConfigBuilder that can be used to create instances of this class.
     *
     * @return A new Builder for this class.
     */
    public static ConfigBuilder<RocksDBConfig> builder() {
        return new ConfigBuilder<>(COMPONENT_CODE, RocksDBConfig::new);
    }

    //endregion
}
