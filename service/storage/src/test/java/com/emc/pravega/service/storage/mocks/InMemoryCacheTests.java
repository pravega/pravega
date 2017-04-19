/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.mocks;

import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.CacheTestBase;
import org.junit.After;
import org.junit.Before;

/**
 * Unit tests for InMemoryCache.
 */
public class InMemoryCacheTests extends CacheTestBase {
    private InMemoryCacheFactory factory;

    @Before
    public void setUp() {
        this.factory = new InMemoryCacheFactory();
    }

    @After
    public void tearDown() {
        if (this.factory != null) {
            this.factory.close();
            this.factory = null;
        }
    }

    @Override
    protected Cache createCache(String cacheId) {
        return this.factory.getCache(cacheId);
    }
}
