package com.emc.pravega.service.server.mocks;

import com.emc.pravega.service.server.reading.CacheKey;
import org.junit.Assert;
import org.junit.Test;

import java.util.function.Consumer;

/**
 * Unit tests for the InMemoryCache class.
 */
public class InMemoryCacheTests {
    private static final int CONTAINER_COUNT = 100;
    private static final int SEGMENT_COUNT = 10;
    private static final int OFFSET_COUNT = 100;
    private static final long OFFSET_MULTIPLIER = 1024 * 1024 * 1024;

    @Test
    public void testFunctionality() {
        InMemoryCache cache = new InMemoryCache();

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
        for (int containerId = 0; containerId < CONTAINER_COUNT; containerId++) {
            for (int segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
                for (long baseOffset = 0; baseOffset < OFFSET_COUNT; baseOffset += 1) {
                    long offset = baseOffset * OFFSET_MULTIPLIER;
                    CacheKey key = new CacheKey(containerId, segmentId, offset);
                    consumer.accept(key);
                }
            }
        }
    }

    private byte[] getData(CacheKey key) {
        return key.getSerialization();
    }
}
