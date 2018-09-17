/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables.hashing;

import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import java.util.Random;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the KeyHash Class.
 */
public class KeyHashTests {
    /**
     * Tests the KeyHash get() and iterator() methods.
     */
    @Test
    public void testFunctionality() {
        val hashConfig = HashConfig.of(1, 2, 4, 8, 16);
        AssertExtensions.assertThrows(
                "Constructor worked with illegal hash length.",
                () -> new KeyHash(new byte[hashConfig.getMinHashLengthBytes() - 1], hashConfig),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);

        byte[] rawHash = new byte[hashConfig.getMinHashLengthBytes() * 2];
        val rnd = new Random(0);
        rnd.nextBytes(rawHash);

        val hash = new KeyHash(rawHash, hashConfig);
        Assert.assertEquals("Unexpected value from hashCount().", hashConfig.getHashCount(), hash.hashCount());
        val iterator = hash.iterator();
        for (int i = 0; i < hash.hashCount(); i++) {
            val eo = hashConfig.getOffsets(i);
            val expected = new ByteArraySegment(rawHash, eo.getLeft(), eo.getRight() - eo.getLeft());
            val actualGet = hash.getPart(i);
            val actualIterator = iterator.next();
            Assert.assertEquals("Unexpected hash length (get).", expected.getLength(), actualGet.getLength());
            Assert.assertEquals("Unexpected hash length (iterator).", expected.getLength(), actualIterator.getLength());
            for (int j = 0; j < actualGet.getLength(); j++) {
                Assert.assertEquals("Unexpected hash contents (get).", expected.get(j), actualGet.get(j));
                Assert.assertEquals("Unexpected hash contents (iterator).", expected.get(j), actualIterator.get(j));
            }
        }

        Assert.assertFalse("Iterator has more elements.", iterator.hasNext());

        AssertExtensions.assertThrows(
                "getPart() worked with illegal argument.",
                () -> hash.getPart(hash.hashCount()),
                ex -> ex instanceof ArrayIndexOutOfBoundsException);
    }
}
