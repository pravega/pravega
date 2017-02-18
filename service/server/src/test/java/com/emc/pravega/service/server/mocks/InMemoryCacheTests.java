/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
    private static final int SEGMENT_COUNT = 1000;
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
