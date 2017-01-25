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

package com.emc.pravega.service.server.mocks;

import com.emc.pravega.service.server.CacheKey;

import lombok.Cleanup;

import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

/**
 * Unit tests for the InMemoryCache class.
 */
public class InMemoryCacheTests {
    private static final String CACHE_ID = "cache_id";
    private static final int SEGMENT_COUNT = 100;
    private static final int OFFSET_COUNT = 100;
    private static final long OFFSET_MULTIPLIER = 1024 * 1024 * 1024;

    @Test
    public void testFunctionality() {
        @Cleanup
        InMemoryCache cache = new InMemoryCache(CACHE_ID);

        // Populate the cache.
        forAllCombinations(key -> cache.insert(key, getData(key)));

        // Retrieve from the cache.
        forAllCombinations(key -> {
            byte[] expectedData = getData(key);
            byte[] actualData = cache.get(key);
            Assert.assertArrayEquals("Unexpected cache contents after insertion.", expectedData, actualData);
        });

        // Remove from the cache.
        forAllCombinations(cache::remove);
        forAllCombinations(key -> Assert.assertNull("Cache still had contents even after key removal.", cache.get(key)));
    }

    private void forAllCombinations(Consumer<CacheKey> consumer) {
        for (int segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
            for (long baseOffset = 0; baseOffset < OFFSET_COUNT; baseOffset += 1) {
                long offset = baseOffset * OFFSET_MULTIPLIER;
                CacheKey key = new CacheKey(segmentId, offset);
                consumer.accept(key);
            }
        }
    }

    private byte[] getData(CacheKey key) {
        return key.getSerialization();
    }
}
