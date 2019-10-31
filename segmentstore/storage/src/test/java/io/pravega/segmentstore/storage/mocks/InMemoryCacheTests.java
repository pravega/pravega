/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage.mocks;

import io.pravega.segmentstore.storage.Cache;
import io.pravega.segmentstore.storage.CacheTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;

/**
 * Unit tests for InMemoryCache.
 */
public class InMemoryCacheTests extends CacheTestBase {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(30);
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
