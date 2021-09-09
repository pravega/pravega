/**
 * Copyright Pravega Authors.
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
package io.pravega.segmentstore.server.tables;

import com.google.common.collect.Maps;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.storage.cache.CacheFullException;
import io.pravega.segmentstore.storage.cache.CacheStorage;
import io.pravega.segmentstore.storage.cache.DirectMemoryCache;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.RequiredArgsConstructor;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.mockito.Mockito;

/**
 * Unit tests for the {@link ContainerKeyCache} class.
 */
public class ContainerKeyCacheTests {
    private static final int SEGMENT_COUNT = 3;
    private static final int KEYS_PER_SEGMENT = 1000;
    private static final KeyHasher KEY_HASHER = KeyHashers.DEFAULT_HASHER;
    @Rule
    public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);
    private CacheStorage cacheStorage;

    @Before
    public void setup() {
        this.cacheStorage = new DirectMemoryCache(Integer.MAX_VALUE);
    }

    @After
    public void tearDown() {
        val s = this.cacheStorage.getState();
        Assert.assertEquals("MEMORY LEAK: Expected CacheStorage to be empty upon closing: " + s, 0, s.getStoredBytes());
        this.cacheStorage.close();
    }

    /**
     * Tests the {@link ContainerKeyCache#includeExistingKey} method.
     */
    @Test
    public void testIncludeExistingKey() {
        @Cleanup
        val keyCache = new ContainerKeyCache(this.cacheStorage);
        val expectedResult = new HashMap<TestKey, CacheBucketOffset>();

        // Insert.
        for (long offset = 0; offset < KEYS_PER_SEGMENT; offset++) {
            // We reuse the same key hash across multiple "segments", to make sure that segmentId does indeed partition
            // the cache.
            val keyHash = newSimpleHash();
            for (long segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
                keyCache.updateSegmentIndexOffset(segmentId, offset);
                long updateResult = keyCache.includeExistingKey(segmentId, keyHash, offset);
                Assert.assertEquals("Unexpected result from includeExistingKey() for new insertion.", offset, updateResult);
                expectedResult.put(new TestKey(segmentId, keyHash), new CacheBucketOffset(offset, false));
            }
        }

        // Verify the cache, after inserts.
        checkCache(expectedResult, keyCache);

        // Perform updates.
        val rnd = new Random(0);
        boolean successfulUpdate = false;
        for (val e : expectedResult.entrySet()) {
            // Every other update will try to set an obsolete offset. We need to verify that such a case will not be accepted.
            successfulUpdate = !successfulUpdate;
            val existingOffset = e.getValue().getSegmentOffset();
            val segmentIndexOffset = keyCache.getSegmentIndexOffset(e.getKey().segmentId);
            long newOffset = existingOffset + 1;
            keyCache.updateSegmentIndexOffset(e.getKey().segmentId, Math.max(segmentIndexOffset, newOffset));

            if (successfulUpdate) {
                long updateResult = keyCache.includeExistingKey(e.getKey().segmentId, e.getKey().keyHash, newOffset);
                Assert.assertEquals("Unexpected result from includeExistingKey() for successful update.", newOffset, updateResult);
                e.setValue(new CacheBucketOffset(newOffset, false));
            } else {
                // Update this Hash's offset with a much higher one.
                val update = TableKeyBatch.update();
                update.add(newTableKey(rnd), e.getKey().keyHash, 1);
                long expectedOffset = keyCache.includeUpdateBatch(e.getKey().segmentId, update, segmentIndexOffset + 1).get(0);
                e.setValue(new CacheBucketOffset(expectedOffset, false));

                // Then verify that includeExistingKey won't modify it.
                long updateResult = keyCache.includeExistingKey(e.getKey().segmentId, e.getKey().keyHash, existingOffset + 1);
                Assert.assertEquals("Unexpected result from includeExistingKey() for obsolete update.", expectedOffset, updateResult);
            }
        }

        // Verify the cache, after updates.
        checkCache(expectedResult, keyCache);

        AssertExtensions.assertThrows(
                "includeExistingKey() accepted negative offset.",
                () -> keyCache.includeExistingKey(0, newSimpleHash(), -1L),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Tests the {@link ContainerKeyCache#includeTailCache} method.
     */
    @Test
    public void testIncludeTailCache() {
        final long segmentId = 0L;
        final long baseOffset = 1000;
        @Cleanup
        val keyCache = new ContainerKeyCache(this.cacheStorage);
        val expectedResult = new HashMap<TestKey, CacheBucketOffset>();

        // Insert some pre-existing values.
        for (int i = 0; i < KEYS_PER_SEGMENT; i++) {
            long offset = baseOffset + i;
            val keyHash = newSimpleHash();
            keyCache.updateSegmentIndexOffset(segmentId, offset);
            long updateResult = keyCache.includeExistingKey(segmentId, keyHash, offset);
            Assert.assertEquals("Unexpected result from includeExistingKey() for new insertion.", offset, updateResult);
            expectedResult.put(new TestKey(segmentId, keyHash), new CacheBucketOffset(offset, false));
        }
        // Perform updates.
        val rnd = new Random(0);
        boolean successfulUpdate = false;
        val updateBatch = new HashMap<UUID, CacheBucketOffset>();
        for (val e : expectedResult.entrySet()) {
            // Every other update will try to set an obsolete offset. We need to verify that such a case will not be accepted.
            successfulUpdate = !successfulUpdate;
            val existingOffset = e.getValue().getSegmentOffset();
            val segmentIndexOffset = keyCache.getSegmentIndexOffset(e.getKey().segmentId);

            long newOffset;
            boolean isRemoved;
            if (successfulUpdate) {
                newOffset = existingOffset + 1;
                isRemoved = existingOffset % 3 == 0; // Need to pick odd number since only odd offsets are successful.
                e.setValue(new CacheBucketOffset(newOffset, isRemoved));
            } else {
                newOffset = existingOffset - 1;
                isRemoved = existingOffset % 4 == 0; // Need to pick even number since only even offsets are unsuccessful.
            }

            updateBatch.put(e.getKey().keyHash, new CacheBucketOffset(newOffset, isRemoved));
        }

        // Update the cache, then check it.
        keyCache.includeTailCache(segmentId, updateBatch);
        checkCache(expectedResult, keyCache);
    }

    /**
     * Tests the {@link ContainerKeyCache#includeUpdateBatch} method for inserts.
     */
    @Test
    public void testBatchInsert() {
        @Cleanup
        val keyCache = new ContainerKeyCache(this.cacheStorage);
        val rnd = new Random(0);
        val expectedResult = new HashMap<TestKey, CacheBucketOffset>();

        long highestOffset = batchInsert(0L, keyCache, expectedResult, rnd);
        checkCache(expectedResult, keyCache);

        // Update all Segment Index Offsets to the max value, which should trigger a migration from the tail cache to
        // the index cache.
        updateSegmentIndexOffsets(keyCache, highestOffset);
        checkNoTailHashes(keyCache);
        checkCache(expectedResult, keyCache);
    }

    /**
     * Tests the {@link ContainerKeyCache#includeUpdateBatch} method for updates.
     */
    @Test
    public void testBatchUpdate() {
        @Cleanup
        val keyCache = new ContainerKeyCache(this.cacheStorage);
        val rnd = new Random(0);
        val expectedResult = new HashMap<TestKey, CacheBucketOffset>();

        // Populate the cache initially.
        long updateOffset = batchInsert(0L, keyCache, expectedResult, rnd);

        // Perform updates
        val updateBatches = new HashMap<Long, TableKeyBatch>();
        long highestOffset = updateOffset;
        for (val e : expectedResult.entrySet()) {
            // Get the batch and calculate the new offset.
            val updateBatch = updateBatches.computeIfAbsent(e.getKey().segmentId, ignored -> TableKeyBatch.update());
            long newOffset = updateOffset + updateBatch.getLength();
            e.setValue(new CacheBucketOffset(newOffset, false));

            // Add to the batch.
            val ignoredKey = newTableKey(rnd);
            updateBatch.add(ignoredKey, e.getKey().keyHash, ignoredKey.getKey().getLength());
            highestOffset = Math.max(highestOffset, newOffset + ignoredKey.getKey().getLength());
        }

        // Apply batches and then verify the cache contents.
        applyBatches(updateBatches, updateOffset, keyCache);
        checkCache(expectedResult, keyCache);

        // Update all Segment Index Offsets to the max value, which should trigger a migration from the tail cache to
        // the index cache.
        updateSegmentIndexOffsets(keyCache, highestOffset);
        checkNoTailHashes(keyCache);
        checkCache(expectedResult, keyCache);
    }

    /**
     * Tests the {@link ContainerKeyCache#includeUpdateBatch} method for removals.
     */
    @Test
    public void testBatchRemove() {
        @Cleanup
        val keyCache = new ContainerKeyCache(this.cacheStorage);
        val rnd = new Random(0);
        val expectedResult = new HashMap<TestKey, CacheBucketOffset>();

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
            e.setValue(new CacheBucketOffset(offset, true));

            // Add to the batch.
            val ignoredKey = newTableKey(rnd);
            removeBatch.add(ignoredKey, e.getKey().keyHash, ignoredKey.getKey().getLength());
            removeOffset2 = Math.max(removeOffset2, offset);
        }

        // Apply batches and then verify the cache contents.
        applyBatches(removeBatches, removeOffset, keyCache);
        checkCache(expectedResult, keyCache);

        // Now remove the rest (and we also remove already deleted items).
        removeBatches.clear();
        long highestOffset = removeOffset2;
        for (val e : expectedResult.entrySet()) {
            // Get the batch and calculate the new offset.
            val removeBatch = removeBatches.computeIfAbsent(e.getKey().segmentId, ignored -> TableKeyBatch.removal());
            e.setValue(new CacheBucketOffset(removeOffset2 + removeBatch.getLength(), true));
            highestOffset = Math.max(highestOffset, e.getValue().getSegmentOffset());

            // Add to the batch.
            val ignoredKey = newTableKey(rnd);
            removeBatch.add(ignoredKey, e.getKey().keyHash, ignoredKey.getKey().getLength());
        }

        // Apply batches and then verify the cache contents.
        applyBatches(removeBatches, removeOffset2, keyCache);
        checkCache(expectedResult, keyCache);

        // Update all Segment Index Offsets to the max value, which should trigger a migration from the tail cache to
        // the index cache.
        updateSegmentIndexOffsets(keyCache, highestOffset + 1);
        checkNoTailHashes(keyCache);
        expectedResult.entrySet().forEach(e -> e.setValue(new CacheBucketOffset(e.getValue().getSegmentOffset(), true)));
        checkCache(expectedResult, keyCache);
    }

    /**
     * Tests the ability to wipe the cache contents upon closing.
     */
    @Test
    public void testClose() {
        final long segmentId = 0;
        val keyHash = newSimpleHash();

        @Cleanup
        val cache1 = new ContainerKeyCache(this.cacheStorage);
        cache1.includeExistingKey(segmentId, keyHash, 0L);
        cache1.close();

        @Cleanup
        val cache2 = new ContainerKeyCache(this.cacheStorage);
        val result2 = cache2.get(segmentId, keyHash);
        Assert.assertNull("Not expecting the cache to have contents after close & reinitialize.", result2);
    }

    /**
     * Test a case when the cache storage throws errors while attempting to update.
     */
    @Test
    public void testCacheUpdateFailure() {
        val segmentId = 0;
        val offset1 = 0L;
        val offset2 = 1L;

        val spiedStorage = Mockito.spy(this.cacheStorage);
        @Cleanup
        val keyCache = new ContainerKeyCache(spiedStorage);
        val expectedResult = new HashMap<TestKey, CacheBucketOffset>();
        val keyHash = newSimpleHash();

        // Perform an initial insert and verify it.
        keyCache.updateSegmentIndexOffset(segmentId, offset1);
        val updateResult1 = keyCache.includeExistingKey(segmentId, keyHash, offset1);
        Assert.assertEquals("Unexpected result from includeExistingKey() for new insertion.", offset1, updateResult1);
        expectedResult.put(new TestKey(segmentId, keyHash), new CacheBucketOffset(offset1, false));
        checkCache(expectedResult, keyCache);

        // For the second insert, fail the cache update and verify that the whole entry has been evicted.
        val replaceAddress = new AtomicInteger(-1);
        Mockito.doAnswer(arg1 -> {
            replaceAddress.set(arg1.getArgument(0));
            throw new CacheFullException("cache full");
        }).when(spiedStorage).replace(Mockito.anyInt(), Mockito.any());

        val deleteAddress = new AtomicInteger(-1);
        Mockito.doAnswer(arg1 -> {
            deleteAddress.set(arg1.getArgument(0));
            return arg1.callRealMethod();
        }).when(spiedStorage).delete(Mockito.anyInt());

        val updateResult2 = keyCache.includeExistingKey(segmentId, keyHash, offset2);
        Assert.assertEquals("Unexpected result from includeExistingKey() for new insertion.", offset2, updateResult2);

        // Set the expected value to null - that indicates it shouldn't be in the cache.
        expectedResult.put(new TestKey(segmentId, keyHash), null);
        checkCache(expectedResult, keyCache);

        Assert.assertNotEquals("Replacement was not attempted.", -1, replaceAddress.get());
        Assert.assertEquals("Deletion was for wrong entry.", replaceAddress.get(), deleteAddress.get());
    }

    /**
     * Tests the ability to perform Cache Eviction, subject to certain rules:
     * - For a segment with no SegmentIndexOffset - there is no cache eviction.
     * - For a segment with SegmentIndexOffset == MAX - eviction is 100% driven by generations.
     * - For a segment with SegmentIndexOffset controlled - eviction is driven by generations and SegmentIndexOffset.
     * <p>
     * The only reason this test should work is because we use a KeyHasher which doesn't produce collisions (for our test);
     * this allows us to stash every Cache Value in its own Cache Entry.
     */
    @Test
    public void testCacheEviction() {
        // We need one segment for each type of rules we are verifying (refer to this test's Javadoc for details).
        final int keyCount = 25;
        final int segmentCount = 3;
        final long segmentIdNoEviction = 0L; // We do not set the Last Index Offset on this one.
        final long segmentIdByGenerations = 1L; // We set the Last Index Offset to Long.MAX_VALUE on this one.
        final long segmentIdByOffset = 2L; // We increment gradually.

        // 1. No SegmentIndexOffset - no cache eviction
        // 2. With SegmentIndexOffset set to MAX - driven by generations.
        // 3. With SegmentIndexOffset controlled - driven by itself.
        @Cleanup
        val keyCache = new ContainerKeyCache(this.cacheStorage);
        val rnd = new Random(0);
        val expectedResult = new HashMap<TestKey, CacheBucketOffset>();

        // Initial cache population. Each Key in each segment gets its own generation.
        for (int i = 0; i < keyCount; i++) {
            // We reuse the same key hash across multiple "segments", to make sure that segmentId does indeed partition
            // the cache.
            keyCache.updateGenerations(i, 0, false);
            val keyHash = KEY_HASHER.hash(newTableKey(rnd).getKey());
            for (long segmentId = 0; segmentId < segmentCount; segmentId++) {
                keyCache.includeExistingKey(segmentId, keyHash, i);
                expectedResult.put(new TestKey(segmentId, keyHash), new CacheBucketOffset(i, false));
            }
        }

        // Set the initial Last Indexed Offsets.
        keyCache.updateSegmentIndexOffset(segmentIdNoEviction, 0L);
        keyCache.updateSegmentIndexOffset(segmentIdByGenerations, Long.MAX_VALUE);
        keyCache.updateSegmentIndexOffset(segmentIdByOffset, 0L);

        val initialStatus = keyCache.getCacheStatus();
        Assert.assertEquals("Unexpected initial oldest generation.", 0, initialStatus.getOldestGeneration());
        Assert.assertEquals("Unexpected initial newest generation.", keyCount - 1, initialStatus.getNewestGeneration());

        // Increase the generations to the newest one, while verifying that at each step we get some removal.
        int ng = initialStatus.getNewestGeneration() + 1;
        for (int og = 1; og <= ng; og++) {
            boolean anythingRemoved = keyCache.updateGenerations(ng, og, false);
            Assert.assertTrue("Expecting something to have been removed (gen).", anythingRemoved);
        }

        // We expect all of these entries to be removed.
        List<TestKey> toRemove = expectedResult.keySet().stream().filter(k -> k.segmentId == segmentIdByGenerations).collect(Collectors.toList());
        toRemove.forEach(hash -> expectedResult.put(hash, null));
        checkNotInCache(toRemove, keyCache);

        // Now update the Last Indexed Offset for a segment and verify that its entries are removed.
        for (long offset = 1; offset <= keyCount; offset++) {
            keyCache.updateSegmentIndexOffset(segmentIdByOffset, offset);
            boolean anythingRemoved = keyCache.updateGenerations(ng, ng, false);
            Assert.assertTrue("Expecting something to have been removed (offset).", anythingRemoved);
        }

        toRemove = expectedResult.keySet().stream().filter(k -> k.segmentId == segmentIdByOffset).collect(Collectors.toList());
        toRemove.forEach(hash -> expectedResult.put(hash, null));
        checkNotInCache(toRemove, keyCache);

        // Verify the final state of the Cache. This should only contain one segment (segmentIdNoEviction).
        checkCache(expectedResult, keyCache);
    }

    /**
     * Tests the {@link SegmentKeyCache} behavior when {@link ContainerKeyCache#updateGenerations} is called with "essentialOnly==false".
     */
    @Test
    public void testNonEssentialCache() {
        // We need one segment for each type of rules we are verifying (refer to this test's Javadoc for details).
        final int keyCount = 25;
        final long segmentId = 0L;

        // Spy on our cache storage and record all insertions.
        val spiedCacheStorage = Mockito.spy(this.cacheStorage);
        val insertCount = new AtomicInteger(0);
        Mockito.doAnswer(arg1 -> {
            insertCount.incrementAndGet();
            return arg1.callRealMethod();
        }).when(spiedCacheStorage).replace(Mockito.anyInt(), Mockito.any());
        Mockito.doAnswer(arg1 -> {
            insertCount.incrementAndGet();
            return arg1.callRealMethod();
        }).when(spiedCacheStorage).insert(Mockito.any());

        @Cleanup
        val keyCache = new ContainerKeyCache(spiedCacheStorage);
        val rnd = new Random(0);
        val expectedResult = new HashMap<TestKey, CacheBucketOffset>();
        val keys = new ArrayList<TableKey>();

        // Initial cache population. Each Key in each segment gets its own generation.
        val currentGeneration = new AtomicInteger(0);
        val segmentOffset = new AtomicLong(0);
        for (int i = 0; i < keyCount; i++) {
            keyCache.updateGenerations(currentGeneration.getAndIncrement(), 0, false);
            val key = newTableKey(rnd);
            keys.add(key);
            val keyHash = KEY_HASHER.hash(key.getKey());
            val offset = segmentOffset.getAndIncrement();
            keyCache.includeExistingKey(segmentId, keyHash, offset);
            expectedResult.put(new TestKey(segmentId, keyHash), new CacheBucketOffset(offset, false));
        }

        checkCache(expectedResult, keyCache);
        Assert.assertEquals("Unexpected number of initial insertions.", expectedResult.size(), insertCount.get());

        // Now mark the cache as non-essential. These updates should effectively evict everything.
        insertCount.set(0);
        for (val k : keys) {
            keyCache.updateGenerations(currentGeneration.getAndIncrement(), 0, true);
            val keyHash = KEY_HASHER.hash(k.getKey());
            keyCache.includeExistingKey(segmentId, keyHash, segmentOffset.getAndIncrement());
        }

        checkNotInCache(expectedResult.keySet(), keyCache);
        Assert.assertEquals("Not expected any cache insertions with cache disabled.", 0, insertCount.get());

        // Re-insert them, with "essential" == false.
        for (val k : keys) {
            keyCache.updateGenerations(currentGeneration.getAndIncrement(), 0, false);
            val keyHash = KEY_HASHER.hash(k.getKey());
            val offset = segmentOffset.getAndIncrement();
            keyCache.includeExistingKey(segmentId, keyHash, offset);
            expectedResult.put(new TestKey(segmentId, keyHash), new CacheBucketOffset(offset, false));
        }

        checkCache(expectedResult, keyCache);
        Assert.assertEquals("Unexpected number of reinsertions.", expectedResult.size(), insertCount.get());
        insertCount.set(0);

        // Evict everything and set the "essential" state.
        keyCache.updateSegmentIndexOffset(segmentId, segmentOffset.get());
        boolean anyEvicted = keyCache.updateGenerations(currentGeneration.getAndIncrement(), currentGeneration.get(), true);
        Assert.assertTrue(anyEvicted);
        checkNotInCache(expectedResult.keySet(), keyCache);
        expectedResult.clear();
        keys.clear();

        // Verify tail migration with the essential offset set (from the previous step).
        val tailKey = newTableKey(rnd);
        val tailKeyHash = KEY_HASHER.hash(tailKey.getKey());
        val tailBatch = TableKeyBatch.update();
        tailBatch.add(tailKey, tailKeyHash, tailKey.getKey().getLength());
        keyCache.includeUpdateBatch(segmentId, tailBatch, segmentOffset.get());
        expectedResult.put(new TestKey(segmentId, tailKeyHash), new CacheBucketOffset(segmentOffset.get(), tailBatch.isRemoval()));
        checkCache(expectedResult, keyCache);

        // The migration should not store this value anywhere in the cache since it's disabled.
        keyCache.updateSegmentIndexOffset(segmentId, segmentOffset.get() + 1);
        checkNotInCache(expectedResult.keySet(), keyCache);
        Assert.assertEquals("Not expected any cache insertions with cache disabled (migration).", 0, insertCount.get());
    }

    /**
     * Tests the ability to record and purge backpointers.
     */
    @Test
    public void testTailCacheMigration() {
        final long segmentId = 1L;
        @Cleanup
        val keyCache = new ContainerKeyCache(this.cacheStorage);
        val rnd = new Random(0);
        val expectedResult = new HashMap<TestKey, CacheBucketOffset>();

        // Insert a number of entries into the cache.
        val allOffsets = new ArrayList<Map.Entry<Long, TestKey>>();
        long batchOffset = 0L;
        for (int i = 0; i < KEYS_PER_SEGMENT; i++) {
            // Create a new batch and record it
            val key = newTableKey(rnd);
            val keyHash = KEY_HASHER.hash(key.getKey());
            val batch = i % 2 == 0 ? TableKeyBatch.update() : TableKeyBatch.removal();
            batch.add(key, keyHash, key.getKey().getLength());
            allOffsets.add(Maps.immutableEntry(batchOffset, new TestKey(segmentId, keyHash)));

            // Apply the batch
            keyCache.includeUpdateBatch(segmentId, batch, batchOffset);
            expectedResult.put(new TestKey(segmentId, keyHash), new CacheBucketOffset(batchOffset, batch.isRemoval()));
            batchOffset = Math.max(batchOffset, batchOffset + batch.getLength());
        }

        // At this point, all entries should be in the tail cache.
        checkCache(expectedResult, keyCache);

        for (val e : allOffsets) {
            long offset = e.getKey();

            // We update the segment index offset to just after this update's offset. This should cause the entry to
            // migrate to the long-term cache, but only if it is not removed.
            keyCache.updateSegmentIndexOffset(segmentId, offset + 1);
            if (!expectedResult.get(e.getValue()).isRemoval()) {
                expectedResult.remove(e.getValue());
            }

            checkCache(expectedResult, keyCache);
        }

        checkNoTailHashes(keyCache);
    }

    /**
     * Test the {@link SegmentKeyCache.MigrationCandidate} class.
     */
    @Test
    public void testMigrationCandidate() {
        val keyCache = new SegmentKeyCache(1L, this.cacheStorage);
        val rnd = new Random(0);

        val batch = TableKeyBatch.update();
        val key = newTableKey(rnd);
        val keyHash = KEY_HASHER.hash(key.getKey());
        batch.add(key, keyHash, key.getKey().getLength());
        keyCache.includeUpdateBatch(batch, 0, 0);

        List<SegmentKeyCache.CacheEntry> evictedEntries = null;
        try {
            // Migrate the tail index to a Cache Entry.
            keyCache.setLastIndexedOffset(batch.getLength() + 1, 1);

            // Evict all entries.
            evictedEntries = keyCache.evictAll();
            Assert.assertEquals(1, evictedEntries.size());
            val entry = evictedEntries.get(0);
            entry.evict(); // Force-evict it from the cache.

            // Check MigrationCandidate.persist().
            val mc = new SegmentKeyCache.MigrationCandidate(keyHash, entry, new CacheBucketOffset(0, false));
            boolean committed = mc.commit(2);
            Assert.assertFalse("Not expected commit() to go through.", committed);
        } finally {
            // Clean up the cache in case of an error. We do not want to leave data hanging around in the Cache.
            if (evictedEntries != null) {
                evictedEntries.forEach(SegmentKeyCache.CacheEntry::evict);
            }
        }
    }

    /**
     * Test the {@link SegmentKeyCache.MigrationCandidate} class when cache is full.
     */
    @Test
    public void testMigrationCandidateFailedCacheFull() {
        @Cleanup
        val fullCache = new DirectMemoryCache(10);
        fullCache.insert(new ByteArraySegment(new byte[(int) (fullCache.getState().getMaxBytes() - fullCache.getBlockAlignment())]));

        val keyCache = new SegmentKeyCache(1L, fullCache);
        val rnd = new Random(0);

        val batch = TableKeyBatch.update();
        val key = newTableKey(rnd);
        val keyHash = KEY_HASHER.hash(key.getKey());
        batch.add(key, keyHash, key.getKey().getLength());
        keyCache.includeUpdateBatch(batch, 0, 0);

        List<SegmentKeyCache.CacheEntry> evictedEntries = null;
        try {
            // Migrate the tail index to a Cache Entry.
            keyCache.setLastIndexedOffset(batch.getLength() + 1, 1);

            // Try to evict all entries.
            evictedEntries = keyCache.evictAll();

            // We expect no evictions because we shouldn't have inserted anything.
            Assert.assertEquals(0, evictedEntries.size());
        } finally {
            // Clean up the cache in case of an error. We do not want to leave data hanging around in the Cache.
            if (evictedEntries != null) {
                evictedEntries.forEach(SegmentKeyCache.CacheEntry::evict);
            }
        }
    }

    private long batchInsert(long insertOffset, ContainerKeyCache keyCache, HashMap<TestKey, CacheBucketOffset> expectedResult, Random rnd) {
        val insertBatches = new HashMap<Long, TableKeyBatch>();
        long highestOffset = 0L;
        for (int i = 0; i < KEYS_PER_SEGMENT; i++) {
            // We reuse the same key hash across multiple "segments", to make sure that segmentId does indeed partition
            // the cache.
            val key = newTableKey(rnd);
            val keyHash = KEY_HASHER.hash(key.getKey());
            for (long segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
                keyCache.updateSegmentIndexOffsetIfMissing(segmentId, () -> 0L);
                val insertBatch = insertBatches.computeIfAbsent(segmentId, ignored -> TableKeyBatch.update());
                val itemOffset = insertOffset + insertBatch.getLength();
                insertBatch.add(key, keyHash, key.getKey().getLength());
                expectedResult.put(new TestKey(segmentId, keyHash), new CacheBucketOffset(itemOffset, false));

                highestOffset = Math.max(highestOffset, itemOffset + key.getKey().getLength());
            }
        }

        applyBatches(insertBatches, insertOffset, keyCache);
        return highestOffset;
    }

    private void applyBatches(HashMap<Long, TableKeyBatch> batchesBySegment, long batchOffset, ContainerKeyCache keyCache) {
        for (val e : batchesBySegment.entrySet()) {
            long segmentId = e.getKey();

            // Collect existing offsets for the update items (so we can check backpointers).
            val previousOffsets = e.getValue().getItems().stream()
                                   .map(i -> keyCache.get(segmentId, i.getHash()))
                                   .collect(Collectors.toList());

            // Fetch initial tail hashes now, before we apply the updates
            val expectedTailHashes = new HashMap<>(keyCache.getTailHashes(segmentId));
            Assert.assertEquals(getExpectedTailUpdateDelta(expectedTailHashes.values()), keyCache.getTailUpdateDelta(segmentId));

            // Update the Cache.
            val batchUpdateResult = keyCache.includeUpdateBatch(segmentId, e.getValue(), batchOffset);

            // Verify update result.
            val expectedOffsets = e.getValue().getItems().stream()
                    .map(i -> batchOffset + i.getOffset())
                    .collect(Collectors.toList());
            AssertExtensions.assertListEquals("Unexpected batch update result.", expectedOffsets, batchUpdateResult, Long::equals);

            // Verify backpointers.
            for (int i = 0; i < expectedOffsets.size(); i++) {
                long sourceOffset = expectedOffsets.get(i);
                CacheBucketOffset prevOffset = previousOffsets.get(i);
                long expectedBackpointer = prevOffset != null ? prevOffset.getSegmentOffset() : -1L;
                long actualBackpointer = keyCache.getBackpointer(segmentId, sourceOffset);
                Assert.assertEquals("Unexpected backpointer for segment " + segmentId + " offset " + sourceOffset,
                        expectedBackpointer, actualBackpointer);
            }

            // Verify tail entries.
            e.getValue().getItems().forEach(i -> expectedTailHashes.put(i.getHash(),
                    new CacheBucketOffset(batchOffset + i.getOffset(), e.getValue().isRemoval())));
            val tailHashes = keyCache.getTailHashes(segmentId);
            Assert.assertEquals("Unexpected Tail Hash count.", expectedTailHashes.size(), tailHashes.size());
            for (val expected : expectedTailHashes.entrySet()) {
                val actual = tailHashes.get(expected.getKey());
                Assert.assertEquals("Unexpected tail hash.", expected.getValue(), actual);
            }
            Assert.assertEquals(getExpectedTailUpdateDelta(expectedTailHashes.values()), keyCache.getTailUpdateDelta(segmentId));
        }
    }

    private int getExpectedTailUpdateDelta(Collection<CacheBucketOffset> tailOffsets) {
        int r = 0;
        for (val c : tailOffsets) {
            if (c.isRemoval()) {
                r--;
            } else {
                r++;
            }
        }
        return r;
    }

    private void updateSegmentIndexOffsets(ContainerKeyCache keyCache, long offset) {
        for (long segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
            keyCache.updateSegmentIndexOffset(segmentId, offset);
        }
    }

    private void checkNoTailHashes(ContainerKeyCache keyCache) {
        for (long segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
            val tailHashes = keyCache.getTailHashes(segmentId);
            Assert.assertTrue("Not expecting any tail hashes.", tailHashes.isEmpty());
        }
    }

    private void checkCache(HashMap<TestKey, CacheBucketOffset> expectedResult, ContainerKeyCache keyCache) {
        for (val e : expectedResult.entrySet()) {
            val result = keyCache.get(e.getKey().segmentId, e.getKey().keyHash);
            if (e.getValue() == null) {
                // No information in the cache about this.
                Assert.assertNull("Unexpected value from get().", result);
            } else {
                // The cache should know about it.
                Assert.assertEquals("Unexpected value from isRemoval().", e.getValue().isRemoval(), result.isRemoval());
                Assert.assertEquals("Unexpected value from getSegmentOffset().", e.getValue().getSegmentOffset(), result.getSegmentOffset());
            }
        }
    }

    private void checkNotInCache(Collection<TestKey> keys, ContainerKeyCache keyCache) {
        for (val e : keys) {
            val result = keyCache.get(e.segmentId, e.keyHash);
            Assert.assertNull("Found key that is not supposed to be in the cache.", result);
        }
    }

    private UUID newSimpleHash() {
        return UUID.randomUUID();
    }

    private TableKey newTableKey(Random rnd) {
        byte[] buf = new byte[rnd.nextInt(100) + 1];
        rnd.nextBytes(buf);
        return TableKey.unversioned(new ByteArraySegment(buf));
    }

    @RequiredArgsConstructor
    private static class TestKey {
        final long segmentId;
        final UUID keyHash;

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
}
