/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *
 */

package com.emc.pravega.service.storage.mocks;

import com.emc.pravega.service.storage.Cache;
import com.emc.pravega.service.storage.CacheTestBase;

/**
 * Unit tests for InMemoryCache.
 */
public class InMemoryCacheTests extends CacheTestBase {
    @Override
    protected Cache createCache(String cacheId) {
        return new InMemoryCache(cacheId);
    }
}
