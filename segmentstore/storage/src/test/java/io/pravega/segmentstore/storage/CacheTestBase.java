/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.storage;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import lombok.Cleanup;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base unit test class for any class implementing the Cache interface.
 */
public abstract class CacheTestBase {
    private static final String CACHE_ID = "cache_id";
    private static final int SEGMENT_COUNT = 1000;
    private static final int OFFSET_COUNT = 100;
    private static final long OFFSET_MULTIPLIER = 1024 * 1024 * 1024;

    protected abstract Cache createCache(String cacheId);

    /**
     * Tests Insert and Get functionality.
     */
    @Test
    public void testInsert() {
        @Cleanup
        Cache cache = createCache(CACHE_ID);

        // Populate the cache.
        forAllCombinations(key -> cache.insert(key, getData(key)));

        // Retrieve from the cache.
        forAllCombinations(key -> {
            byte[] expectedData = getData(key);
            byte[] actualData = cache.get(key);
            Assert.assertArrayEquals("Unexpected cache contents after insertion.", expectedData, actualData);
        });
    }

    /**
     * Verifies that Remove actually removes from the cache.
     */
    @Test
    public void testRemove() {
        @Cleanup
        Cache cache = createCache(CACHE_ID);

        // Populate the cache.
        forAllCombinations(key -> cache.insert(key, getData(key)));

        // Remove from the cache.
        forAllCombinations(key -> {
            cache.remove(key);
            Assert.assertNull("Cache still had contents after removing key.", cache.get(key));
        });
    }

    /**
     * Verifies that the cache is cleared when closing & reopening.
     */
    @Test
    public void testClearOnOpen() {
        // Populate in cache 1.
        try (Cache cache = createCache(CACHE_ID)) {
            forAllCombinations(key -> cache.insert(key, getData(key)));
        }

        // Attempt to reopen it.
        try (Cache cache = createCache(CACHE_ID)) {
            forAllCombinations(key -> Assert.assertNull("Cache still had contents after closing and reopening.", cache.get(key)));
        }
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
        return key.serialize();
    }

    //region CacheKey

    @RequiredArgsConstructor
    @EqualsAndHashCode(callSuper = false)
    private static class CacheKey extends Cache.Key {
        private final long segmentId;
        private final long offset;

        @Override
        public byte[] serialize() {
            ByteBuffer bb = ByteBuffer.allocate(Long.BYTES * 2);
            bb.putLong(this.segmentId);
            bb.putLong(this.offset);
            return bb.array();
        }
    }

    //endregion
}
