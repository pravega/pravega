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

import io.pravega.common.util.HashedArray;
import io.pravega.segmentstore.server.tables.hashing.HashConfig;
import io.pravega.segmentstore.server.tables.hashing.KeyHash;
import io.pravega.segmentstore.server.tables.hashing.KeyHasher;
import java.util.Comparator;
import java.util.Map;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link BucketUpdate} class.
 */
public class BucketUpdateTests {
    /**
     * Tests general functionality.
     */
    @Test
    public void testFunctionality() {
        int count = 5;
        val bucket = TableBucket.builder().build();
        val bu = new BucketUpdate(bucket);
        Assert.assertEquals("Unexpected bucket.", bucket, bu.getBucket());
        Assert.assertFalse("Not expecting any updates at this time.", bu.hasUpdates());

        for (int i = 0; i < count; i++) {
            bu.withExistingKey(new KeyInfo(new HashedArray(new byte[]{(byte) i}), i));
            bu.withKeyUpdate(new KeyUpdate(new HashedArray(new byte[]{(byte) -i}), i, i % 2 == 0));
        }

        Assert.assertTrue("Unexpected result from isKeyUpdated for updated key.",
                bu.isKeyUpdated(new HashedArray(new byte[]{(byte) -1})));
        Assert.assertFalse("Unexpected result from isKeyUpdated for non-updated key.",
                bu.isKeyUpdated(new HashedArray(new byte[]{(byte) -count})));

        Assert.assertEquals("Unexpected existing keys count.", count, bu.getExistingKeys().size());
        Assert.assertEquals("Unexpected updated keys count.", count, bu.getKeyUpdates().size());

        val existingIterator = bu.getExistingKeys().stream().sorted(Comparator.comparingLong(KeyInfo::getOffset)).iterator();
        val updatesIterator = bu.getKeyUpdates().stream().sorted(Comparator.comparingLong(KeyInfo::getOffset)).iterator();
        for (int i = 0; i < count; i++) {
            val e = existingIterator.next();
            val u = updatesIterator.next();
            Assert.assertEquals("Unexpected key for existing " + i, (byte) i, e.getKey().array()[0]);
            Assert.assertEquals("Unexpected offset for existing " + i, i, e.getOffset());
            Assert.assertEquals("Unexpected key for update " + i, (byte) -i, u.getKey().array()[0]);
            Assert.assertEquals("Unexpected offset for update " + i, i, u.getOffset());
            Assert.assertEquals("Unexpected value for isDeleted " + i, i % 2 == 0, u.isDeleted());
        }
    }

    /**
     * Tests the {@link BucketUpdate#groupByHash} method.
     */
    @Test
    public void testGroupByHash() {
        int count = 5;
        val bucket = TableBucket.builder().build();
        val bu = new BucketUpdate(bucket);
        for (int i = 0; i < count; i++) {
            bu.withExistingKey(new KeyInfo(new HashedArray(new byte[]{(byte) i}), i));
            bu.withKeyUpdate(new KeyUpdate(new HashedArray(new byte[]{(byte) i}), i + 1, i % 2 == 0));
        }

        // We define a binary hasher, that hashes based on the parity of the first byte in the key.
        val binaryHasher = KeyHasher.custom(key -> new byte[]{(byte) (key.get(0) % 2)}, HashConfig.of(1));

        // Group using this hasher.
        val groups = bu.groupByHash(binaryHasher::hash);

        // Verify correctness.
        Assert.assertEquals("Unexpected number of groups.", 2, groups.size());
        testHashGroup(groups, binaryHasher.hash(new byte[]{0}), bucket, binaryHasher);
        testHashGroup(groups, binaryHasher.hash(new byte[]{1}), bucket, binaryHasher);
    }

    private void testHashGroup(Map<KeyHash, BucketUpdate> groups, KeyHash hash, TableBucket bucket, KeyHasher hasher) {
        val g = groups.get(hash);
        Assert.assertEquals("Unexpected bucket.", bucket, g.getBucket());
        Assert.assertNotNull("Couldn't find the group for hash 0.", g);
        for (val info : g.getExistingKeys()) {
            Assert.assertEquals("Not expecting this key.", hash, hasher.hash(info.getKey()));
        }

        for (val update : g.getExistingKeys()) {
            Assert.assertEquals("Not expecting this update.", hash, hasher.hash(update.getKey()));
        }
    }
}
