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
import io.pravega.common.hash.HashHelper;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.contracts.AttributeReference;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateByReference;
import io.pravega.segmentstore.contracts.Attributes;
import io.pravega.segmentstore.contracts.BadAttributeUpdateException;
import io.pravega.segmentstore.contracts.ReadResult;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentInformation;
import io.pravega.segmentstore.server.DirectSegmentAccess;
import io.pravega.segmentstore.server.tables.hashing.HashConfig;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.jute.Index;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link IndexReader} and {@link IndexWriter} classes.
 */
public class IndexReaderWriterTests extends ThreadPooledTestSuite {
    //region Test Hashers

    // Default Hashing uses a SHA512 hashing, which produces very few collisions. This is the same as used in the non-test
    // version of the code.
    private static final HashConfig DEFAULT_HASH_CONFIG = HashConfig.of(16, 12, 12, 12, 12);
    private static final KeyHasher DEFAULT_HASHER = KeyHasher.sha512(DEFAULT_HASH_CONFIG);

    // Collision Hashing "bucketizes" the SHA512 hash into much smaller buckets (1 byte each), with each accepting a very
    // narrow range [0..4) of values. This will produce a lot of collisions, which helps us test adjustable tree depth and
    // collision resolutions in the indexer.
    private static final HashConfig COLLISION_HASH_CONFIG = HashConfig.of(1, 1, 1, 1, 1);
    private static final int COLLISION_HASH_BASE = 4;
    private static final int COLLISION_HASH_BUCKETS = (int) Math.pow(COLLISION_HASH_BASE, COLLISION_HASH_CONFIG.getMinHashLengthBytes());
    private static final KeyHasher COLLISION_HASHER = KeyHasher.custom(IndexReaderWriterTests::hashWithCollisions, COLLISION_HASH_CONFIG);

    private static byte[] hashWithCollisions(ArrayView arrayView) {
        // Generate SHA512 hash.
        val baseHash = DEFAULT_HASHER.hash(arrayView);

        // "Bucketize" it into COLLISION_HASH_BUCKETS buckets (i.e., hash value will be in interval [0..COLLISION_HASH_BUCKETS)).
        byte[] result = new byte[COLLISION_HASH_CONFIG.getMinHashLengthBytes()];
        int hashValue = HashHelper.seededWith(IndexReaderWriterTests.class.getName()).hashToBucket(baseHash.getArray(), COLLISION_HASH_BUCKETS);

        // Split the hash value into parts, and generate the final hash.
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) (hashValue % COLLISION_HASH_BASE);
            hashValue /= COLLISION_HASH_BASE;
        }

        // At the end, we should have a COLLISION_HASH_CONFIG.getMinHashLengthBytes() hash, with each hash part being 1 byte
        // containing values from 0 (inclusive) until COLLISION_HASH_BASE (exclusive).
        return result;
    }

    //endregion

    private static final int MAX_KEY_LENGTH = 512;
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

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
        val w = newWriter();
        val updates = w.generateInitialTableAttributes();
        val values = updates.stream().collect(Collectors.toMap(AttributeUpdate::getAttributeId, AttributeUpdate::getValue));
        Assert.assertEquals("Unexpected number of updates generated.", 2, values.size());
        Assert.assertEquals("Unexpected value for TableNodeID.", 1L, (long) values.get(Attributes.TABLE_NODE_ID));
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
        val bucketsToKeys = new HashMap<TableBucket, ArrayList<KeyUpdate>>();
        val rnd = new Random(0);
        for (int i = 0; i < bucketCount; i++) {
            val bucket = TableBucket.builder()
                                    .node(new TableBucket.Node(false, UUID.randomUUID(), i))
                                    .build();

            // Keep track of all KeyUpdates for this bucket.
            val keyUpdates = new ArrayList<KeyUpdate>();
            bucketsToKeys.put(bucket, keyUpdates);

            // Generate keys, and record them where needed.
            for (int j = 0; j < hashesPerBucket; j++) {
                byte[] key = new byte[DEFAULT_HASH_CONFIG.getMinHashLengthBytes() * 4];
                keyUpdates.add(new KeyUpdate(new HashedArray(key), i * hashesPerBucket + j, true));
                rnd.nextBytes(key);
                hashToBuckets.put(DEFAULT_HASHER.hash(key), bucket);
            }
        }

        // Group updates by bucket. Since we override locateBucket, we do not need a segment access, hence safe to pass null.
        val w = new CustomLocateBucketIndexer(DEFAULT_HASHER, executorService(), hashToBuckets);
        val allKeyUpdates = new ArrayList<KeyUpdate>();
        bucketsToKeys.values().forEach(allKeyUpdates::addAll);
        val bucketUpdates = w.groupByBucket(allKeyUpdates, null, new TimeoutTimer(TIMEOUT)).join();

        Assert.assertEquals("Unexpected number of Bucket Updates.", bucketCount, bucketUpdates.size());
        for (BucketUpdate bu : bucketUpdates) {
            Assert.assertTrue("Not expecting Existing Keys to be populated.", bu.getExistingKeys().isEmpty());
            ArrayList<KeyUpdate> expected = bucketsToKeys.get(bu.getBucket());
            Assert.assertNotNull("Found extra bucket.", expected);
            AssertExtensions.assertContainsSameElements("Unexpected updates grouped.",
                    expected, bu.getKeyUpdates(),
                    (u1, u2) -> u1.getKey().equals(u2.getKey()) && u1.getOffset() == u2.getOffset() ? 0 : 1);
        }
    }

    //endregion

    //region IndexReader and IndexWriter combined tests

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBucket} and {@link IndexReader#getBackpointerOffset}
     * methods for a Single Bucket update (no removal)
     */
    @Test
    public void testIncrementalUpdate() {
        testIncrementalUpdate(DEFAULT_HASHER);
        //testIncrementalUpdate(COLLISION_HASHER); TODO uncomment
    }

    private void testIncrementalUpdate(KeyHasher hasher) {
        int keyCount = COLLISION_HASH_BUCKETS * 10;

        val rnd = new Random(0);
        val timer = new TimeoutTimer(TIMEOUT);
        val w = newWriter(hasher);
        val segment = newMock(w);

        // Start with an empty bucket (lookup). Add a number of distinct keys.
        // At each step, lookup their buckets & backpointers and verify result correctness.
        long offset = 0;
        val keys = new ArrayList<HashedArray>();
        for (int i = 0; i < keyCount; i++) {
            val key = newKey(rnd);
            keys.add(key);
            System.out.println(String.format("Insert %s: %s.", i, key));
            offset = updateKey(key, offset, w, hasher, segment, timer);
        }

        // Update the keys. At each step, lookup their buckets & backpointers and verify result correctness.
//        for (val key : keys) { TODO uncomment
//            offset = updateKey(key, offset, w, hasher, segment, timer);
//        }

        // At the end, verify all the keys (use groupByBucket and getbackpointers).
    }

    private long updateKey(HashedArray key, long currentOffset, IndexWriter w, KeyHasher hasher, DirectSegmentAccess segment, TimeoutTimer timer) {
        // Update the key.
        val update = new KeyUpdate(key, currentOffset, false);
        long newOffset = currentOffset + 2 * key.getLength();
        val updates = w.groupByBucket(Collections.singleton(update), segment, timer).join();
        val attrCount = w.updateBuckets(updates, segment, currentOffset, newOffset, TIMEOUT).join();

        // Locate the newly added key and verify it can be retrieved properly. The new key should be the last in the
        // Bucket so the bucket should be pointing to it.
        val keyHash = hasher.hash(key);
        val bucket = w.locateBucket(keyHash, segment, timer).join();
        val lastBucketNode = bucket.getLastNode();
        Assert.assertNotNull("No buckets after insert.", lastBucketNode);
        Assert.assertFalse("Not expecting a partial Table Bucket.", lastBucketNode.isIndexNode());
        Assert.assertEquals("", currentOffset, w.getOffset(lastBucketNode));
        return newOffset;
    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBucket} and {@link IndexReader#getBackpointerOffset}
     * methods for a Single Bucket update that removes it.
     */
    @Test
    public void testIncrementalRemove() {

    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBucket} and {@link IndexReader#getBackpointerOffset}
     * methods for multiple bucket updates (no removals).
     */
    @Test
    public void testBulkUpdate() {

    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBucket} and {@link IndexReader#getBackpointerOffset}
     * methods for multiple bucket updates made of removals.
     */
    @Test
    public void testBulkRemove() {

    }

    //endregion

    private IndexReader newReader() {
        return new IndexReader(executorService());
    }

    private IndexWriter newWriter() {
        return newWriter(DEFAULT_HASHER);
    }

    private IndexWriter newWriter(KeyHasher hasher) {
        return new IndexWriter(hasher, executorService());
    }

    private HashedArray newKey(Random rnd) {
        byte[] key = new byte[Math.max(1, rnd.nextInt(MAX_KEY_LENGTH))];
        rnd.nextBytes(key);
        return new HashedArray(key);
    }

    private SegmentAttributeMock newMock(IndexWriter writer){
        val mock = new SegmentAttributeMock();
        mock.updateAttributes(writer.generateInitialTableAttributes(), TIMEOUT).join();
        return mock;
    }

    //region CustomLocateBucketIndexer

    /**
     * IndexWriter where the locateBucket method has been overridden to return specific values.
     */
    private static class CustomLocateBucketIndexer extends IndexWriter {
        private final Map<KeyHash, TableBucket> buckets;

        CustomLocateBucketIndexer(KeyHasher keyHasher, ScheduledExecutorService executor, Map<KeyHash, TableBucket> buckets) {
            super(keyHasher, executor);
            this.buckets = buckets;
        }

        @Override
        public CompletableFuture<TableBucket> locateBucket(KeyHash hash, DirectSegmentAccess segment, TimeoutTimer timer) {
            return CompletableFuture.completedFuture(buckets.get(hash));
        }
    }

    //endregion

    //region SegmentAttributeMock

    /**
     * {@link DirectSegmentAccess} implementation that only handles attribute updates and retrievals. This accurately mocks
     * the behavior of the entire Segment Container with respect to Attributes, without dealing with all the complexities
     * behind the actual implementation.
     */
    @ThreadSafe
    private class SegmentAttributeMock implements DirectSegmentAccess {
        @GuardedBy("attributes")
        private final HashMap<UUID, Long> attributes = new HashMap<>();

        @Override
        public CompletableFuture<Map<UUID, Long>> getAttributes(Collection<UUID> attributeIds, boolean cache, Duration timeout) {
            return CompletableFuture.supplyAsync(() -> {
                synchronized (this.attributes) {
                    return attributeIds.stream()
                            .collect(Collectors.toMap(id -> id, id -> this.attributes.getOrDefault(id, Attributes.NULL_ATTRIBUTE_VALUE)));
                }
            }, executorService());
        }

        @Override
        public CompletableFuture<Void> updateAttributes(Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            return CompletableFuture.runAsync(() -> {
                synchronized (this.attributes) {
                    attributeUpdates.forEach(this::updateAttribute);
                }
            }, executorService());
        }

        @GuardedBy("attributes")
        @SneakyThrows(BadAttributeUpdateException.class)
        private void updateAttribute(AttributeUpdate update) {
            if (update instanceof AttributeUpdateByReference) {
                AttributeUpdateByReference updateByRef = (AttributeUpdateByReference) update;
                AttributeReference<UUID> idRef = updateByRef.getIdReference();
                if (idRef != null) {
                    updateByRef.setAttributeId(getReferenceValue(idRef, updateByRef));
                }

                AttributeReference<Long> valueRef = updateByRef.getValueReference();
                if (valueRef != null) {
                    updateByRef.setValue(getReferenceValue(valueRef, updateByRef));
                }
            }

            long newValue = update.getValue();
            boolean hasValue = false;
            long previousValue = Attributes.NULL_ATTRIBUTE_VALUE;
            if (this.attributes.containsKey(update.getAttributeId())) {
                hasValue = true;
                previousValue = this.attributes.get(update.getAttributeId());
            }

            switch (update.getUpdateType()) {
                case ReplaceIfGreater:
                    if (hasValue && newValue <= previousValue) {
                        throw new BadAttributeUpdateException("Segment", update, false, "GreaterThan");
                    }

                    break;
                case ReplaceIfEquals:
                    if (update.getComparisonValue() != previousValue || !hasValue) {
                        throw new BadAttributeUpdateException("Segment", update, !hasValue, "ReplaceIfEquals");
                    }

                    break;
                case None:
                    if (hasValue) {
                        throw new BadAttributeUpdateException("Segment", update, false, "NoUpdate");
                    }

                    break;
                case Accumulate:
                    if (hasValue) {
                        newValue += previousValue;
                        update.setValue(newValue);
                    }

                    break;
                case Replace:
                    break;
                default:
                    throw new BadAttributeUpdateException("Segment", update, !hasValue, "Unsupported");
            }

            this.attributes.put(update.getAttributeId(), update.getValue());
        }

        @GuardedBy("attributes")
        private <T> T getReferenceValue(AttributeReference<T> ref, AttributeUpdateByReference updateByRef) throws BadAttributeUpdateException {
            UUID attributeId = ref.getAttributeId();
            if (this.attributes.containsKey(attributeId)) {
                return ref.getTransformation().apply(this.attributes.get(attributeId));
            } else {
                throw new BadAttributeUpdateException("Segment", updateByRef, true, "AttributeRef");
            }
        }

        //region Unused methods

        @Override
        public long getSegmentId() {
            return 0;
        }

        @Override
        public CompletableFuture<Long> append(byte[] data, Collection<AttributeUpdate> attributeUpdates, Duration timeout) {
            throw new UnsupportedOperationException("append");
        }

        @Override
        public ReadResult read(long offset, int maxLength, Duration timeout) {
            throw new UnsupportedOperationException("read");
        }

        @Override
        public SegmentProperties getInfo() {
            throw new UnsupportedOperationException("getInfo");
        }

        @Override
        public CompletableFuture<Long> seal(Duration timeout) {
            throw new UnsupportedOperationException("seal");
        }

        @Override
        public CompletableFuture<Void> truncate(long offset, Duration timeout) {
            throw new UnsupportedOperationException("offset");
        }

        //endregion
    }

    //endregion
}
