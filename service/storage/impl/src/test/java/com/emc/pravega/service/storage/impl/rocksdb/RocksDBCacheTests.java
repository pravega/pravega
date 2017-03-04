/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.rocksdb;

import com.emc.pravega.common.util.ConfigurationException;
import com.emc.pravega.common.util.PropertyBag;
import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.CacheTestBase;
import com.google.common.io.Files;
import java.io.File;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import org.junit.After;
import org.junit.Before;

/**
 * Unit tests for RocksDBCache.
 */
public class RocksDBCacheTests extends CacheTestBase {
    private final AtomicReference<File> tempDir = new AtomicReference<>();
    private final AtomicReference<RocksDBConfig> config = new AtomicReference<>();
    private final AtomicReference<RocksDBCacheFactory> factory = new AtomicReference<>();

    @Before
    public void before() {
        this.tempDir.set(Files.createTempDir());
        this.config.set(new TestConfig().withDatabaseDir(tempDir.get().getAbsolutePath()));
        this.factory.set(new RocksDBCacheFactory(this.config.get()));
    }

    @After
    public void after() {
        this.factory.getAndSet(null).close();
        this.tempDir.getAndSet(null).delete();
    }

    @Override
    protected Cache createCache(String cacheId) {
        return this.factory.get().getCache(cacheId);
    }

    private static class TestConfig extends RocksDBConfig {
        @Getter
        private String databaseDir;

        TestConfig() throws ConfigurationException {
            super(PropertyBag.create());
        }

        TestConfig withDatabaseDir(String value) {
            this.databaseDir = value;
            return this;
        }
    }
}
