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
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link KeyUpdateCollection} class.
 */
public class KeyUpdateCollectionTests {
    /**
     * Tests the {@link KeyUpdateCollection#add} method.
     */
    @Test
    public void testAdd() {
        final int keyCount = 10;
        final int duplication = 2;
        val rnd = new Random(0);
        long maxOffset = 0;
        val allUpdates = new ArrayList<BucketUpdate.KeyUpdate>();
        val c = new KeyUpdateCollection();
        for (int i = 0; i < keyCount; i++) {
            byte[] key = new byte[i + 1];
            rnd.nextBytes(key);
            for (int j = 0; j < duplication; j++) {
                long offset = 1 + rnd.nextInt(10000);
                int length = key.length + rnd.nextInt(200);
                long version = offset / 2 + (i % 2 == j % 2 ? 1 : -1);
                val update = new BucketUpdate.KeyUpdate(new HashedArray(key), offset, version, false);
                c.add(update, length);
                maxOffset = Math.max(maxOffset, offset + length);
                allUpdates.add(update);
            }
        }

        Assert.assertEquals("Unexpected TotalUpdateCount.", keyCount * duplication, c.getTotalUpdateCount());
        Assert.assertEquals("Unexpected LastIndexedOffset.", maxOffset, c.getLastIndexedOffset());

        val allByKey = allUpdates.stream().collect(Collectors.groupingBy(BucketUpdate.KeyInfo::getKey));
        val expected = allByKey.values().stream()
                .map(l -> l.stream().max(Comparator.comparingLong(BucketUpdate.KeyInfo::getVersion)).get())
                .sorted(Comparator.comparingLong(BucketUpdate.KeyInfo::getOffset))
                .collect(Collectors.toList());
        val actual = c.getUpdates().stream()
                .sorted(Comparator.comparingLong(BucketUpdate.KeyInfo::getOffset))
                .collect(Collectors.toList());

        AssertExtensions.assertListEquals("Unexpected result from getUpdates().", expected, actual,
                (u1, u2) -> u1.getKey().equals(u2.getKey()) && u1.getVersion() == u2.getVersion() && u1.getOffset() == u2.getOffset());
    }
}
