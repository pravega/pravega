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

import com.google.common.io.Files;
import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.CacheTestBase;
import java.io.File;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;

/**
 * Unit tests for RocksDBCache.
 */
public class RocksDBCacheTests extends CacheTestBase {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(30);
    private final AtomicReference<File> tempDir = new AtomicReference<>();
    private final AtomicReference<RocksDBConfig> config = new AtomicReference<>();
    private final AtomicReference<RocksDBCacheFactory> factory = new AtomicReference<>();

    @Before
    public void setUp() {
        this.tempDir.set(Files.createTempDir());
        this.config.set(RocksDBConfig.builder().with(RocksDBConfig.DATABASE_DIR, tempDir.get().getAbsolutePath()).build());
        this.factory.set(new RocksDBCacheFactory(this.config.get()));
    }

    @After
    public void tearDown() {
        this.factory.getAndSet(null).close();
        this.tempDir.getAndSet(null).delete();
    }

    @Override
    protected Cache createCache(String cacheId) {
        return this.factory.get().getCache(cacheId);
    }
}
