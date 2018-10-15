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

import io.pravega.common.TimeoutTimer;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the {@link IndexReader} and {@link IndexWriter} classes.
 */
public class IndexReaderWriterTests extends ThreadPooledTestSuite {
    private static final int KEY_COUNT = KeyHashers.COLLISION_HASH_BUCKETS * 10;
    private static final int UPDATE_BATCH_SIZE = 1000;
    private static final int REMOVE_BATCH_SIZE = 1000;
    private static final int MAX_KEY_LENGTH = 512;
    private static final long NO_OFFSET = -1L;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    @Rule
    public Timeout globalTimeout = new Timeout(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

    @Override
    protected int getThreadPoolSize() {
        return 3;
    }

    //region IndexReader specific tests

    /**
     * Test the {@link IndexReader#getLastIndexedOffset(SegmentProperties)} method.
     */
    @Test
    public void testGetLastIndexedOffset() {
        val ir = newReader();
        Assert.assertEquals("Unexpected value when attribute is not present.",
                0, ir.getLastIndexedOffset(StreamSegmentInformation.builder().name("s").build()));
        val si = StreamSegmentInformation.builder().name("s")
                                         .attributes(Collections.singletonMap(Attributes.TABLE_INDEX_OFFSET, 123456L))
                                         .build();
        Assert.assertEquals("Unexpected value when attribute present.",
                123456, ir.getLastIndexedOffset(si));
    }

    /**
     * Tests the {@link IndexReader#getOffset(TableBucket.Node)} method.
     */
    @Test
    public void testGetOffset() {
        long validValue = 1L;
        val ir = newReader();
        val ac = new AttributeCalculator();
        AssertExtensions.assertThrows(
                "getOffset() accepted an Index node.",
                () -> ir.getOffset(new TableBucket.Node(true, UUID.randomUUID(), validValue)),
                ex -> ex instanceof IllegalArgumentException);

        // We generate a data node but purposefully assign it an Index Node value, since that actually encodes the value
        // slightly differently and we want to make sure the getOffset() method calls the correct APIs to decode it.
        val dataNode = new TableBucket.Node(false, UUID.randomUUID(), ac.getIndexNodeAttributeValue(validValue));
        val offset = ir.getOffset(dataNode);
        Assert.assertEquals("Unexpected result from getOffset().", validValue, offset);
    }

    //endregion

    //region IndexWriter specific tests

    /**
     * Tests the {@link IndexWriter#generateInitialTableAttributes()} method.
     */
    @Test
    public void testGenerateInitialTableAttributes() {
        val updates = IndexWriter.generateInitialTableAttributes();
        val values = updates.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue));
        Assert.assertEquals("Unexpected number of updates generated.", 2, values.size());
        Assert.assertEquals("Unexpected value for TableNodeID.", 0L, (long) values.get(Attributes.TABLE_NODE_ID));
        Assert.assertEquals("Unexpected value for TableIndexOffset.", 0L, (long) values.get(Attributes.TABLE_INDEX_OFFSET));
    }

    /**
     * Tests the {@link IndexWriter#groupByBucket} method.
     */
    @Test
    public void testGroupByBucket() {
        int bucketCount = 5;
        int hashesPerBucket = 5;
        val hashToBuckets = new HashMap<KeyHash, TableBucket>();
        val bucketsToKeys = new HashMap<TableBucket, ArrayList<BucketUpdate.KeyUpdate>>();
        val rnd = new Random(0);
        for (int i = 0; i < bucketCount; i++) {
            val bucket = TableBucket.builder()
                                    .node(new TableBucket.Node(false, UUID.randomUUID(), i))
                                    .build();

            // Keep track of all KeyUpdates for this bucket.
            val keyUpdates = new ArrayList<BucketUpdate.KeyUpdate>();
            bucketsToKeys.put(bucket, keyUpdates);

            // Generate keys, and record them where needed.
            for (int j = 0; j < hashesPerBucket; j++) {
                byte[] key = new byte[KeyHashers.HASH_CONFIG.getMinHashLengthBytes() * 4];
                keyUpdates.add(new BucketUpdate.KeyUpdate(new HashedArray(key), i * hashesPerBucket + j, true));
                rnd.nextBytes(key);
                hashToBuckets.put(KeyHashers.DEFAULT_HASHER.hash(key), bucket);
            }
        }

        // Group updates by bucket. Since we override locateBucket, we do not need a segment access, hence safe to pass null.
        val w = new CustomLocateBucketIndexer(KeyHashers.DEFAULT_HASHER, executorService(), hashToBuckets);
        val allKeyUpdates = new ArrayList<BucketUpdate.KeyUpdate>();
        bucketsToKeys.values().forEach(allKeyUpdates::addAll);
        val bucketUpdates = w.groupByBucket(allKeyUpdates, null, new TimeoutTimer(TIMEOUT)).join();

        Assert.assertEquals("Unexpected number of Bucket Updates.", bucketCount, bucketUpdates.size());
        for (BucketUpdate bu : bucketUpdates) {
            Assert.assertTrue("Not expecting Existing Keys to be populated.", bu.getExistingKeys().isEmpty());
            val expected = bucketsToKeys.get(bu.getBucket());
            Assert.assertNotNull("Found extra bucket.", expected);
            AssertExtensions.assertContainsSameElements("Unexpected updates grouped.",
                    expected, bu.getKeyUpdates(),
                    (u1, u2) -> u1.getKey().equals(u2.getKey()) && u1.getOffset() == u2.getOffset() ? 0 : 1);
        }
    }

    //endregion

    //region IndexReader and IndexWriter combined tests

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBuckets} and {@link IndexReader#getBackpointerOffset}
     * methods for updating one entry at a time using a hasher that's not prone to collisions.
     */
    @Test
    public void testIncrementalUpdate() {
        testUpdate(KeyHashers.DEFAULT_HASHER, 1);
    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBuckets} and {@link IndexReader#getBackpointerOffset}
     * methods for updating one entry at a time using a hasher that's very prone to collisions.
     */
    @Test
    public void testIncrementalUpdateCollisions() {
        testUpdate(KeyHashers.COLLISION_HASHER, 1);
    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBuckets} and {@link IndexReader#getBackpointerOffset}
     * methods for updating multiple entries at a time using a hasher that's not prone to collisions.
     */
    @Test
    public void testBulkUpdate() {
        testUpdate(KeyHashers.DEFAULT_HASHER, UPDATE_BATCH_SIZE);
    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBuckets} and {@link IndexReader#getBackpointerOffset}
     * methods for updating multiple entries at a time using a hasher that's very prone to collisions.
     */
    @Test
    public void testBulkUpdateCollisions() {
        testUpdate(KeyHashers.COLLISION_HASHER, UPDATE_BATCH_SIZE);
    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBuckets} and {@link IndexReader#getBackpointerOffset}
     * methods for removing a single entry at a time using a hasher that's not prone to collisions.
     */
    @Test
    public void testIncrementalRemove() {
        testRemove(KeyHashers.DEFAULT_HASHER, 1);
    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBuckets} and {@link IndexReader#getBackpointerOffset}
     * methods for removing a single entry at a time using a hasher that's very prone to collisions.
     */
    @Test
    public void testIncrementalRemoveCollisions() {
        testRemove(KeyHashers.COLLISION_HASHER, 1);
    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBuckets} and {@link IndexReader#getBackpointerOffset}
     * methods for removing multiple entries at a time using a hasher that's not prone to collisions.
     */
    @Test
    public void testBulkRemove() {
        testRemove(KeyHashers.DEFAULT_HASHER, REMOVE_BATCH_SIZE);
    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBuckets} and {@link IndexReader#getBackpointerOffset}
     * methods for removing multiple entries at a time using a hasher that's very prone to collisions.
     */
    @Test
    public void testBulkRemoveCollisions() {
        testRemove(KeyHashers.COLLISION_HASHER, REMOVE_BATCH_SIZE);
    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBuckets} and {@link IndexReader#getBackpointerOffset}
     * methods for updating and removing entries using a hasher that's not prone to collisions.
     */
    @Test
    public void testUpdateRemove() {
        testUpdateAndRemove(KeyHashers.DEFAULT_HASHER);
    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBuckets} and {@link IndexReader#getBackpointerOffset}
     * methods for updating and removing entries using a hasher that's very prone to collisions.
     */
    @Test
    public void testUpdateRemoveCollisions() {
        testUpdateAndRemove(KeyHashers.COLLISION_HASHER);
    }

    //endregion

    //region Helpers

    private void testUpdate(KeyHasher hasher, int updateBatchSize) {
        val rnd = new Random(0);
        val w = newWriter(hasher);
        val segment = newMock();

        // Generate batches and update them at once.
        long offset = 0;
        val keys = new HashMap<Long, HashedArray>();
        while (keys.size() < KEY_COUNT) {
            int batchSize = Math.min(updateBatchSize, KEY_COUNT - keys.size());
            val batch = generateUpdateBatch(batchSize, offset, rnd);
            offset = updateKeys(batch, w, keys, segment);
        }

        // Verify index.
        checkIndex(keys.values(), keys, w, hasher, segment);

        // Update the keys using the requested batch size.
        val toUpdate = new ArrayList<HashedArray>(keys.values());
        int i = 0;
        while (i < toUpdate.size()) {
            val batch = new HashMap<HashedArray, Long>();
            int batchSize = Math.min(updateBatchSize, toUpdate.size() - i);
            int batchOffset = 0;
            while (batch.size() < batchSize) {
                val key = toUpdate.get(i);
                batch.put(key, encodeOffset(offset + batchOffset, false));
                batchOffset += key.getLength();
                i++;
            }

            offset = updateKeys(batch, w, keys, segment);
        }

        // Verify index.
        checkIndex(keys.values(), keys, w, hasher, segment);
    }

    private void testRemove(KeyHasher hasher, int removeBatchSize) {
        val rnd = new Random(0);
        val w = newWriter(hasher);
        val segment = newMock();

        // Bulk-insert all the keys.
        val keys = new HashMap<Long, HashedArray>();
        val updateBatch = generateUpdateBatch(KEY_COUNT, 0, rnd);
        long offset = updateKeys(updateBatch, w, keys, segment);

        // Remove the keys using the requested batch size.
        val toRemove = new ArrayList<HashedArray>(keys.values());
        int i = 0;
        while (i < toRemove.size()) {
            val batch = new HashMap<HashedArray, Long>();
            int batchSize = Math.min(removeBatchSize, toRemove.size() - i);
            int batchOffset = 0;
            while (batch.size() < batchSize) {
                val key = toRemove.get(i);
                batch.put(key, encodeOffset(offset + batchOffset, true));
                batchOffset += key.getLength();
                i++;
            }

            offset = updateKeys(batch, w, keys, segment);
        }

        // Verify index.
        checkIndex(toRemove, keys, w, hasher, segment);

        // Since we removed all nodes, we are not expecting any collisions left, so no backpointers.
        checkNoBackpointers(segment);

        // Verify that all surviving nodes are index nodes. We do this by figuring out how many times we incremented
        // TABLE_NODE_ID, accounting for the fact that we have number of non-index attributes too and that it starts
        // at some predefined value.
        val initialAttribs = IndexWriter.generateInitialTableAttributes();
        int expectedAttributeCount = w.getTableNodeId(segment.getInfo())
                + initialAttribs.size()
                - (int) initialAttribs.stream().filter(a -> a.getAttributeId() == Attributes.TABLE_NODE_ID).findFirst().get().getValue();
        int attributeCount = segment.getAttributeCount();
        Assert.assertEquals("Unexpected number of nodes left after complete removal.", expectedAttributeCount, attributeCount);
    }

    private void testUpdateAndRemove(KeyHasher hasher) {
        final int batchSizeBase = 200;
        final int iterationCount = 200; // This should be smaller than batchSizeBase.
        val rnd = new Random(0);
        val w = newWriter(hasher);
        val segment = newMock();

        // Generate batches and update them at once.
        long offset = 0;
        val existingKeys = new HashMap<Long, HashedArray>();
        val allKeys = new HashSet<HashedArray>();
        int maxUpdateBatchSize = batchSizeBase + 1;
        int maxRemoveBatchSize = 1;
        for (int i = 0; i < iterationCount; i++) {
            // Insert/Update a set of keys. With every iteration, we update fewer and fewer.
            int updateBatchSize = rnd.nextInt(maxUpdateBatchSize) + 1;
            val updateBatch = generateUpdateBatch(updateBatchSize, offset, rnd);
            offset = updateKeys(updateBatch, w, existingKeys, segment);
            allKeys.addAll(updateBatch.keySet());

            // Remove a set of keys. With every iteration, we remove more and more.
            // Pick existing keys at random, and delete them.
            int removeBatchSize = rnd.nextInt(maxRemoveBatchSize) + 1;
            val removeBatch = new HashMap<HashedArray, Long>();
            val remainingKeys = new ArrayList<HashedArray>(existingKeys.values());
            int batchOffset = 0;
            while (removeBatch.size() < removeBatchSize && removeBatch.size() < remainingKeys.size()) {
                HashedArray key;
                do {
                    key = remainingKeys.get(rnd.nextInt(remainingKeys.size()));
                } while (removeBatch.containsKey(key));

                removeBatch.put(key, encodeOffset(offset + batchOffset, true));
            }

            // Pick a non-existing key, and add it too.
            HashedArray nonExistingKey;
            do {
                byte[] b = new byte[rnd.nextInt(MAX_KEY_LENGTH) + 1];
                rnd.nextBytes(b);
                nonExistingKey = new HashedArray(b);
            } while (allKeys.contains(nonExistingKey));
            removeBatch.put(nonExistingKey, encodeOffset(offset + batchOffset, true));

            // Apply the removal.
            offset = updateKeys(removeBatch, w, existingKeys, segment);
            maxUpdateBatchSize -= batchSizeBase / iterationCount;
            maxRemoveBatchSize += batchSizeBase / iterationCount;
        }

        // Verify index.
        checkIndex(allKeys, existingKeys, w, hasher, segment);
    }

    private long updateKeys(Map<HashedArray, Long> keysWithOffset, IndexWriter w, HashMap<Long, HashedArray> existingKeys, SegmentMock segment) {
        val timer = new TimeoutTimer(TIMEOUT);

        val keyUpdates = keysWithOffset.entrySet().stream()
                                       .map(e -> new BucketUpdate.KeyUpdate(e.getKey(), decodeOffset(e.getValue()), isRemoveOffset(e.getValue())))
                                       .sorted(Comparator.comparingLong(BucketUpdate.KeyUpdate::getOffset))
                                       .collect(Collectors.toList());

        // This is the value that we will set TABLE_INDEX_NODE to. It is not any key's offset (and we don't really care what its value is)
        long firstKeyOffset = keyUpdates.get(0).getOffset();
        long postIndexOffset = keyUpdates.get(keyUpdates.size() - 1).getOffset() + 2 * MAX_KEY_LENGTH;

        // Generate the BucketUpdate for the key.
        val bucketUpdates = w.groupByBucket(keyUpdates, segment, timer).join();

        // Fetch existing keys.
        val oldOffsets = new ArrayList<Long>();
        for (val bu : bucketUpdates) {
            w.getBucketOffsets(bu.getBucket(), segment, timer).join()
             .forEach(offset -> {
                 HashedArray existingKey = existingKeys.getOrDefault(offset, null);
                 Assert.assertNotNull("Existing bucket points to non-existing key.", existingKey);
                 bu.withExistingKey(new BucketUpdate.KeyInfo(existingKey, offset));

                // Key replacement; remove this offset.
                if (keysWithOffset.containsKey(existingKey)) {
                    oldOffsets.add(offset);
                }
             });
        }

        // Apply the updates.
        int prevNodeId = w.getTableNodeId(segment.getInfo());
        val attrCount = w.updateBuckets(bucketUpdates, segment, firstKeyOffset, postIndexOffset, TIMEOUT).join();
        int newIndexNodeCount = w.getTableNodeId(segment.getInfo()) - prevNodeId;
        Assert.assertEquals("Unexpected number of index nodes added.", newIndexNodeCount, (int) attrCount);

        // Record the key as being updated.
        oldOffsets.forEach(existingKeys::remove);
        keysWithOffset.forEach((key, offset) -> {
            if (isRemoveOffset(offset)) {
                existingKeys.remove(decodeOffset(offset), key);
            } else {
                existingKeys.put(decodeOffset(offset), key);
            }
        });
        return postIndexOffset;
    }

    private void checkIndex(Collection<HashedArray> allKeys, Map<Long, HashedArray> existingKeysByOffset, IndexWriter w,
                            KeyHasher hasher, SegmentMock segment) {
        val timer = new TimeoutTimer(TIMEOUT);

        // Group all keys by their full hash (each hash should translate to a bucket), and make sure they're ordered by
        // offset (in descending order - so we can verify backpointer ordering).
        val existingKeys = existingKeysByOffset.entrySet().stream()
                                               .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        val keysByHash = allKeys.stream()
                                .map(key -> new BucketUpdate.KeyInfo(key, existingKeys.getOrDefault(key, NO_OFFSET)))
                                .sorted((k1, k2) -> Long.compare(k2.getOffset(), k1.getOffset())) // Reverse order.
                                .collect(Collectors.groupingBy(keyInfo -> hasher.hash(keyInfo.getKey())));
        val buckets = w.locateBuckets(keysByHash.keySet(), segment, timer).join();
        for (val e : keysByHash.entrySet()) {
            val hash = e.getKey();
            val keys = e.getValue();
            val bucket = buckets.get(hash);
            Assert.assertNotNull("No bucket found for hash " + hash, bucket);
            boolean allDeleted = keys.stream().allMatch(k -> k.getOffset() == NO_OFFSET);
            Assert.assertEquals("Only expecting partial bucket when all its keys are deleted " + hash, allDeleted, bucket.isPartial());
            val bucketOffsets = w.getBucketOffsets(bucket, segment, timer).join();

            // Verify that we didn't return too many or too few keys.
            if (allDeleted) {
                Assert.assertEquals("Not expecting any offsets to be returned for bucket: " + hash, 0, bucketOffsets.size());
            } else {
                AssertExtensions.assertGreaterThan("Expected at least one offset to be returned for bucket: " + hash, 0, bucketOffsets.size());
            }

            AssertExtensions.assertLessThanOrEqual("Too many offsets returned for bucket: " + hash, keys.size(), bucketOffsets.size());

            // Verify returned keys are as expected.
            for (int i = 0; i < bucketOffsets.size(); i++) {
                long actualOffset = bucketOffsets.get(i);
                long expectedOffset = keys.get(i).getOffset();
                String id = String.format("{%s[%s]}", hash, i);

                // In this loop, we do not expect to have Deleted Keys. If our Expected Offset indicates this key should
                // have been deleted, then getBucketOffsets() should not have returned this.
                Assert.assertNotEquals("Expecting a deleted key but found existing one: " + id, NO_OFFSET, expectedOffset);
                Assert.assertEquals("Unexpected key offset in bucket " + id, expectedOffset, actualOffset);
            }

            if (bucketOffsets.size() < keys.size()) {
                val prevKeyOffset = keys.get(bucketOffsets.size()).getOffset();
                Assert.assertEquals("Missing key from bucket " + hash, NO_OFFSET, prevKeyOffset);
            }
        }
    }

    private void checkNoBackpointers(SegmentMock segment) {
        val ac = new AttributeCalculator();
        int count = segment.getAttributeCount((id, value) -> ac.isBackpointerAttributeKey(id) && value != Attributes.NULL_ATTRIBUTE_VALUE);
        Assert.assertEquals("Not expecting any backpointers.", 0, count);
    }

    private HashMap<HashedArray, Long> generateUpdateBatch(int batchSize, long offset, Random rnd) {
        val batch = new HashMap<HashedArray, Long>();
        int batchOffset = 0;

        // Randomly generated keys may be duplicated, so we need to loop as long as we need to fill up the batch.
        while (batch.size() < batchSize) {
            val key = newKey(rnd);
            batch.put(key, encodeOffset(offset + batchOffset, false));
            batchOffset += key.getLength();
        }

        return batch;
    }

    private IndexReader newReader() {
        return new IndexReader(executorService());
    }

    private IndexWriter newWriter(KeyHasher hasher) {
        return new IndexWriter(hasher, executorService());
    }

    private HashedArray newKey(Random rnd) {
        byte[] key = new byte[Math.max(1, rnd.nextInt(MAX_KEY_LENGTH))];
        rnd.nextBytes(key);
        return new HashedArray(key);
    }

    private SegmentMock newMock() {
        val mock = new SegmentMock(executorService());
        mock.updateAttributes(IndexWriter.generateInitialTableAttributes(), TIMEOUT).join();
        return mock;
    }

    private long encodeOffset(long offset, boolean isRemove) {
        offset++;
        return isRemove ? -offset : offset;
    }

    private long decodeOffset(long encodedOffset) {
        return (encodedOffset < 0 ? -encodedOffset : encodedOffset) - 1;
    }

    private boolean isRemoveOffset(long encodedOffset) {
        return encodedOffset < 0;
    }

    //endregion

    //region CustomLocateBucketIndexer

    /**
     * IndexWriter where the locateBuckets method has been overridden to return specific values.
     */
    private static class CustomLocateBucketIndexer extends IndexWriter {
        private final Map<KeyHash, TableBucket> buckets;

        CustomLocateBucketIndexer(KeyHasher keyHasher, ScheduledExecutorService executor, Map<KeyHash, TableBucket> buckets) {
            super(keyHasher, executor);
            this.buckets = buckets;
        }

        @Override
        public CompletableFuture<Map<KeyHash, TableBucket>> locateBuckets(Collection<KeyHash> keyHashes, DirectSegmentAccess segment, TimeoutTimer timer) {
            return CompletableFuture.completedFuture(
                    keyHashes.stream().collect(Collectors.toMap(k -> k, buckets::get)));
        }
    }

    //endregion
}
