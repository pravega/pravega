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
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import io.pravega.segmentstore.storage.mocks.InMemoryCacheFactory;
import io.pravega.test.common.AssertExtensions;
import java.util.HashMap;
import java.util.List;
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
    private static final int SEGMENT_COUNT = 3;
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
     * Tests the {@link ContainerKeyCache#updateBatch} method for inserts.
     */
    @Test
    public void testBatchInsert() {
        @Cleanup
        val cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        val keyCache = new ContainerKeyCache(CONTAINER_ID, cacheFactory);
        val rnd = new Random(0);
        val expectedResult = new HashMap<TestKey, ContainerKeyCache.GetResult>();

        batchInsert(0L, keyCache, expectedResult, rnd);
        checkCache(expectedResult, keyCache);
    }

    /**
     * Tests the {@link ContainerKeyCache#updateBatch} method for updates.
     */
    @Test
    public void testBatchUpdate() {
        @Cleanup
        val cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        val keyCache = new ContainerKeyCache(CONTAINER_ID, cacheFactory);
        val rnd = new Random(0);
        val expectedResult = new HashMap<TestKey, ContainerKeyCache.GetResult>();

        // Populate the cache initially.
        long updateOffset = batchInsert(0L, keyCache, expectedResult, rnd);

        // Perform updates
        val updateBatches = new HashMap<Long, TableKeyBatch>();
        long removeOffset = updateOffset;
        for (val e : expectedResult.entrySet()) {
            // Get the batch and calculate the new offset.
            val updateBatch = updateBatches.computeIfAbsent(e.getKey().segmentId, ignored -> TableKeyBatch.update());
            long newOffset = updateOffset + updateBatch.getLength();
            e.setValue(new ContainerKeyCache.GetResult(newOffset, true));

            // Add to the batch.
            val ignoredKey = newTableKey(rnd);
            updateBatch.add(ignoredKey, (KeyHash) e.getKey().keyHash, ignoredKey.getKey().getLength());
            removeOffset = Math.max(removeOffset, newOffset + ignoredKey.getKey().getLength());
        }

        // Apply batches and then verify the cache contents.
        applyBatches(updateBatches, updateOffset, keyCache);
        checkCache(expectedResult, keyCache);
    }

    /**
     * Tests the {@link ContainerKeyCache#updateBatch} method for removals.
     */
    @Test
    public void testBatchRemove() {
        @Cleanup
        val cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        val keyCache = new ContainerKeyCache(CONTAINER_ID, cacheFactory);
        val rnd = new Random(0);
        val expectedResult = new HashMap<TestKey, ContainerKeyCache.GetResult>();

        // Populate the cache initially.
        long removeOffset = batchInsert(0L, keyCache, expectedResult, rnd);

        // Remove half the items.
        val removeBatches = new HashMap<Long, TableKeyBatch>();
        boolean remove = false;
        long removeOffset2 = removeOffset;
        for (val e : expectedResult.entrySet()) {
            // We only remove half of the items.
            remove = !remove;
            if (!remove) {
                continue;
            }

            // Get the batch and calculate the new offset.
            val removeBatch = removeBatches.computeIfAbsent(e.getKey().segmentId, ignored -> TableKeyBatch.removal());
            long offset = removeOffset + removeBatch.getLength();
            e.setValue(new ContainerKeyCache.GetResult(offset, false));

            // Add to the batch.
            val ignoredKey = newTableKey(rnd);
            removeBatch.add(ignoredKey, (KeyHash) e.getKey().keyHash, ignoredKey.getKey().getLength());
            removeOffset2 = Math.max(removeOffset2, offset);
        }

        // Apply batches and then verify the cache contents.
        applyBatches(removeBatches, removeOffset, keyCache);
        checkCache(expectedResult, keyCache);

        // Now remove the rest (and we also remove already deleted items).
        removeBatches.clear();
        for (val e : expectedResult.entrySet()) {
            // Get the batch and calculate the new offset.
            val removeBatch = removeBatches.computeIfAbsent(e.getKey().segmentId, ignored -> TableKeyBatch.removal());
            e.setValue(new ContainerKeyCache.GetResult(removeOffset2 + removeBatch.getLength(), false));

            // Add to the batch.
            val ignoredKey = newTableKey(rnd);
            removeBatch.add(ignoredKey, (KeyHash) e.getKey().keyHash, ignoredKey.getKey().getLength());
        }

        // Apply batches and then verify the cache contents.
        applyBatches(removeBatches, removeOffset2, keyCache);
        checkCache(expectedResult, keyCache);
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

    /**
     * Tests the ability to perform Cache Eviction, subject to certain rules:
     * - For a segment with no SegmentIndexOffset - there is no cache eviction.
     * - For a segment with SegmentIndexOffset == MAX - eviction is 100% driven by generations.
     * - For a segment with SegmentIndexOffset controlled - eviction is driven by generations and SegmentIndexOffset.
     *
     * The only reason this test should work is because we use a KeyHasher which doesn't produce collisions (for our test);
     * this allows us to stash every Cache Value in its own Cache Entry.
     */
    @Test
    public void testCacheEviction() {
        // We need one segment for each type of rules we are verifying (refer to this test's Javadoc for details).
        final int keyCount = 100;
        final int segmentCount = 3;
        final long segmentIdNoEviction = 0L; // We do not set the Last Index Offset on this one.
        final long segmentIdByGenerations = 1L; // We set the Last Index Offset to Long.MAX_VALUE on this one.
        final long segmentIdByOffset = 2L; // We increment gradually.

        // 1. No SegmentIndexOffset - no cache eviction
        // 2. With SegmentIndexOffset set to MAX - driven by generations.
        // 3. With SegmentIndexOffset controlled - driven by itself.
        @Cleanup
        val cacheFactory = new InMemoryCacheFactory();
        @Cleanup
        val keyCache = new ContainerKeyCache(CONTAINER_ID, cacheFactory);
        val rnd = new Random(0);
        val expectedResult = new HashMap<TestKey, ContainerKeyCache.GetResult>();

        // Initial cache population. Each Key in each segment gets its own generation.
        for (int i = 0; i < keyCount; i++) {
            // We reuse the same key hash across multiple "segments", to make sure that segmentId does indeed partition
            // the cache.
            keyCache.updateGenerations(i, 0);
            val keyHash = KEY_HASHER.hash(newTableKey(rnd).getKey());
            for (long segmentId = 0; segmentId < segmentCount; segmentId++) {
                keyCache.updateKey(segmentId, keyHash, (long) i);
                expectedResult.put(new TestKey(segmentId, keyHash), new ContainerKeyCache.GetResult(i, true));
            }
        }

        // Set the initial Last Indexed Offsets.
        keyCache.setSegmentIndexOffset(segmentIdNoEviction, -1L);
        keyCache.setSegmentIndexOffset(segmentIdByGenerations, Long.MAX_VALUE);
        keyCache.setSegmentIndexOffset(segmentIdByOffset, 0L);

        val initialStatus = keyCache.getCacheStatus();
        Assert.assertEquals("Unexpected initial oldest generation.", 0, initialStatus.getOldestGeneration());
        Assert.assertEquals("Unexpected initial newest generation.", keyCount - 1, initialStatus.getNewestGeneration());
        AssertExtensions.assertGreaterThan("Expecting a non-zero cache size.", 0, initialStatus.getSize());

        // Increase the generations to the newest one, while verifying that at each step we get some removal.
        int ng = initialStatus.getNewestGeneration() + 1;
        for (int og = 1; og <= ng; og++) {
            long sizeRemoved = keyCache.updateGenerations(ng, og);
            AssertExtensions.assertGreaterThan("Expecting something to have been removed (gen).", 0, sizeRemoved);
        }

        // We expect all of these entries to be removed.
        List<TestKey> toRemove = expectedResult.keySet().stream().filter(k -> k.segmentId == segmentIdByGenerations).collect(Collectors.toList());
        toRemove.forEach(expectedResult::remove);
        checkNotInCache(toRemove, keyCache);

        // Now update the Last Indexed Offset for a segment and verify that its entries are removed.
        for (long offset = 1; offset <= keyCount; offset++) {
            keyCache.setSegmentIndexOffset(segmentIdByOffset, offset);
            long sizeRemoved = keyCache.updateGenerations(ng, ng);
            AssertExtensions.assertGreaterThan("Expecting something to have been removed (offset).", 0, sizeRemoved);
        }

        toRemove = expectedResult.keySet().stream().filter(k -> k.segmentId == segmentIdByOffset).collect(Collectors.toList());
        toRemove.forEach(expectedResult::remove);
        checkNotInCache(toRemove, keyCache);

        // Verify the final state of the Cache. This should only contain one segment (segmentIdNoEviction).
        checkCache(expectedResult, keyCache);
    }

    private long batchInsert(long insertOffset, ContainerKeyCache keyCache, HashMap<TestKey, ContainerKeyCache.GetResult> expectedResult, Random rnd) {
        val insertBatches = new HashMap<Long, TableKeyBatch>();
        long highestOffset = 0L;
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

                highestOffset = Math.max(highestOffset, itemOffset + key.getKey().getLength());
            }
        }

        applyBatches(insertBatches, insertOffset, keyCache);
        return highestOffset;
    }

    private void applyBatches(HashMap<Long, TableKeyBatch> batchesBySegment, long batchOffset, ContainerKeyCache keyCache) {
        for (val e : batchesBySegment.entrySet()) {
            val batchUpdateResult = keyCache.updateBatch(e.getKey(), e.getValue(), batchOffset);
            val expectedOffsets = e.getValue().getItems().stream()
                    .map(i -> batchOffset + i.getOffset())
                    .collect(Collectors.toList());
            AssertExtensions.assertListEquals("Unexpected batch update result.", expectedOffsets, batchUpdateResult, Long::equals);
        }
    }

    private void checkCache(HashMap<TestKey, ContainerKeyCache.GetResult> expectedResult, ContainerKeyCache keyCache) {
        for (val e : expectedResult.entrySet()) {
            val result = keyCache.get(e.getKey().segmentId, e.getKey().keyHash);
            Assert.assertEquals("Unexpected value from isPresent().", e.getValue().isPresent(), result.isPresent());
            Assert.assertEquals("Unexpected value from getSegmentOffset().", e.getValue().getSegmentOffset(), result.getSegmentOffset());
        }
    }

    private void checkNotInCache(List<TestKey> keys, ContainerKeyCache keyCache) {
        for (val e : keys) {
            val result = keyCache.get(e.segmentId, e.keyHash);
            Assert.assertNull("Found key that is not supposed to be in the cache.", result);
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
