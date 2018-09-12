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
import io.pravega.segmentstore.server.tables.hashing.HashConfig;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.ThreadPooledTestSuite;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link IndexReader} and {@link IndexWriter} classes.
 */
public class IndexReaderWriterTests extends ThreadPooledTestSuite {
    private static final HashConfig DEFAULT_HASH_CONFIG = HashConfig.of(16, 12, 12, 12, 12);
    private static final KeyHasher DEFAULT_HASHER = KeyHasher.sha512(DEFAULT_HASH_CONFIG);
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
    public void testUpdateSingleBucket() {
        // TODO: these tests should be run with collision-prone hashers as well, not just with sha512. Maybe run them twice.
    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBucket} and {@link IndexReader#getBackpointerOffset}
     * methods for a Single Bucket update that removes it.
     */
    @Test
    public void testRemoveSingleBucket() {

    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBucket} and {@link IndexReader#getBackpointerOffset}
     * methods for multiple bucket updates (no removals).
     */
    @Test
    public void testUpdateMultiBucket() {

    }

    /**
     * Tests the {@link IndexWriter#updateBuckets}, {@link IndexReader#locateBucket} and {@link IndexReader#getBackpointerOffset}
     * methods for multiple bucket updates made of removals.
     */
    @Test
    public void testRemovals() {

    }

    //endregion

    private IndexReader newReader() {
        return new IndexReader(executorService());
    }

    private IndexWriter newWriter() {
        return new IndexWriter(DEFAULT_HASHER, executorService());
    }

    //region Helper Classes

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
        public CompletableFuture<TableBucket> locateBucket(DirectSegmentAccess segment, KeyHash hash, TimeoutTimer timer) {
            return CompletableFuture.completedFuture(buckets.get(hash));
        }
    }

    //endregion
}
