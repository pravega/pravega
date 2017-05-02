/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.service.storage.mocks;

import io.pravega.service.storage.Cache;
import io.pravega.service.storage.CacheTestBase;
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
