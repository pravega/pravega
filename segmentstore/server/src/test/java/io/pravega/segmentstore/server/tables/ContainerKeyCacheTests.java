/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import io.pravega.segmentstore.storage.mocks.InMemoryCacheFactory;
import io.pravega.test.common.AssertExtensions;
import java.util.HashMap;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link ContainerKeyCache} class.
 */
public class ContainerKeyCacheTests {
    private static final int CONTAINER_ID = 1;
    private static final int SEGMENT_COUNT = 2;
    private static final int KEYS_PER_SEGMENT = 1000;
    private static final KeyHasher KEY_HASHER = KeyHashers.DEFAULT_HASHER;
    private static final int SIMPLE_HASH_LENGTH = 64; // We use this to test sub-grouping inside the Cache Entry.
    private static final int HASH_HASHCODE_BUCKETS = 10; // This sub-groups the KeyHashes into smaller buckets (to test grouping).

    /**
     * Tests the {@link ContainerKeyCache#updateKey} method.
     */
    @Test
    public void testUpdateKey() {
        @Cleanup
        val cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        val keyCache = new ContainerKeyCache(CONTAINER_ID, cacheFactory);
        val rnd = new Random(0);
        val expectedResult = new HashMap<TestKey, ContainerKeyCache.GetResult>();

        // Insert.
        for (int i = 0; i < KEYS_PER_SEGMENT; i++) {
            // We reuse the same key hash across multiple "segments", to make sure that segmentId does indeed partition
            // the cache.
            val keyHash = newSimpleHash(rnd);
            for (long segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
                long offset = i;
                long updateResult = keyCache.updateKey(segmentId, keyHash, offset);
                Assert.assertEquals("Unexpected result from updateKey() for new insertion.", offset, updateResult);
                expectedResult.put(new TestKey(segmentId, keyHash), new ContainerKeyCache.GetResult(offset, true));
            }
        }

        // Verify the cache, after inserts.
        checkCache(expectedResult, keyCache);

        // Perform updates.
        boolean successfulUpdate = false;
        for (val e : expectedResult.entrySet()) {
            // Every other update will try to set an obsolete offset. We need to verify that such a case will not be accepted.
            successfulUpdate = !successfulUpdate;
            val existingOffset = e.getValue().getSegmentOffset();
            long newOffset = successfulUpdate ? existingOffset + 1 : Math.max(0, existingOffset - 1);
            long updateResult = keyCache.updateKey(e.getKey().segmentId, e.getKey().keyHash, newOffset);
            if (successfulUpdate) {
                Assert.assertEquals("Unexpected result from updateKey() for successful update.", newOffset, updateResult);
                e.setValue(new ContainerKeyCache.GetResult(newOffset, true));
            } else {
                Assert.assertEquals("Unexpected result from updateKey() for obsolete update.", existingOffset, updateResult);
            }
        }

        // Verify the cache, after updates.
        checkCache(expectedResult, keyCache);

        AssertExtensions.assertThrows(
                "updateKey() accepted negative offset.",
                () -> keyCache.updateKey(0, newSimpleHash(rnd), -1L),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Tests the {@link ContainerKeyCache#updateBatch} method.
     */
    @Test
    public void testUpdateBatch() {
        @Cleanup
        val cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        val keyCache = new ContainerKeyCache(CONTAINER_ID, cacheFactory);
        val rnd = new Random(0);
        val expectedResult = new HashMap<TestKey, ContainerKeyCache.GetResult>();

        // Generate initial insert batches.
        val insertBatches = new HashMap<Long, TableKeyBatch>();
        final long insertOffset = 0L;
        for (int i = 0; i < KEYS_PER_SEGMENT; i++) {
            // We reuse the same key hash across multiple "segments", to make sure that segmentId does indeed partition
            // the cache.
            val key = newTableKey(rnd);
            val keyHash = KEY_HASHER.hash(key.getKey());
            for (long segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
                val insertBatch = insertBatches.computeIfAbsent(segmentId, ignored -> TableKeyBatch.update());
                val itemOffset = insertOffset + insertBatch.getLength();
                insertBatch.add(key, keyHash, key.getKey().getLength());
                expectedResult.put(new TestKey(segmentId, keyHash), new ContainerKeyCache.GetResult(itemOffset, true));
            }
        }

        // Apply insert batches.
        for (val e : insertBatches.entrySet()) {
            val batchUpdateResult = keyCache.updateBatch(e.getKey(), e.getValue(), insertOffset);
            val expectedOffsets = e.getValue().getItems().stream()
                                   .map(i -> (long) i.getOffset())
                                   .collect(Collectors.toList());
            AssertExtensions.assertListEquals("Unexpected batch insert result.", expectedOffsets, batchUpdateResult, Long::equals);
        }

        // Verify the cache, after inserts.
        checkCache(expectedResult, keyCache);

        // TODO updates and removals.
    }

    /**
     * Tests the ability to wipe the cache contents upon closing.
     */
    @Test
    public void testClose() {
        final long segmentId = 0;
        val keyHash = newSimpleHash(new Random(0));

        @Cleanup
        val cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        val cache1 = new ContainerKeyCache(CONTAINER_ID, cacheFactory);
        cache1.updateKey(segmentId, keyHash, 1L);
        cache1.close();

        @Cleanup
        val cache2 = new ContainerKeyCache(CONTAINER_ID, cacheFactory);
        val result2 = cache2.get(segmentId, keyHash);
        Assert.assertNull("Not expecting the cache to have contents after close & reinitialize.", result2);
    }

    @Test
    public void testCacheClient(){
        // Include some API to update segmentIndexOffsets.
    }

    private void checkCache(HashMap<TestKey, ContainerKeyCache.GetResult> expectedResult, ContainerKeyCache keyCache) {
        for (val e : expectedResult.entrySet()) {
            val result = keyCache.get(e.getKey().segmentId, e.getKey().keyHash);
            Assert.assertEquals("Unexpected value from isPresent().", e.getValue().isPresent(), result.isPresent());
            Assert.assertEquals("Unexpected value from getSegmentOffset().", e.getValue().getSegmentOffset(), result.getSegmentOffset());
        }
    }

    private HashedArray newSimpleHash(Random rnd) {
        byte[] buf = new byte[SIMPLE_HASH_LENGTH];
        rnd.nextBytes(buf);
        return new TestHashedArray(buf);
    }

    private TableKey newTableKey(Random rnd) {
        byte[] buf = new byte[rnd.nextInt(100) + 1];
        rnd.nextBytes(buf);
        return TableKey.unversioned(new ByteArraySegment(buf));
    }

    @RequiredArgsConstructor
    private static class TestKey {
        final long segmentId;
        final HashedArray keyHash;

        @Override
        public int hashCode() {
            return this.keyHash.hashCode() ^ Long.hashCode(this.segmentId);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TestKey) {
                TestKey ti = (TestKey) obj;
                return this.segmentId == ti.segmentId
                        && this.keyHash.equals(ti.keyHash);
            }

            return false;
        }
    }

    private static class TestHashedArray extends HashedArray {
        TestHashedArray(byte[] array) {
            super(array);
        }

        @Override
        public int hashCode() {
            return super.hashCode() % HASH_HASHCODE_BUCKETS;
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj);
        }
    }
}
