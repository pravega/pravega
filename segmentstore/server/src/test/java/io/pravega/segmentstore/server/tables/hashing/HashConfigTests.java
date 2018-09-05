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

import io.pravega.test.common.AssertExtensions;
import java.util.Arrays;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the HashConfig class.
 */
public class HashConfigTests {
    /**
     * Tests the constructor with an invalid input.
     */
    @Test
    public void testInvalidInput() {
        AssertExtensions.assertThrows(
                "HashConfig.of() allowed empty argument.",
                () -> HashConfig.of(new int[0]),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "HashConfig.of() allowed length 0.",
                () -> HashConfig.of(new int[1]),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Tests the getHashCount(), getMinHashLengthBytes() and getOffsets().
     */
    @Test
    public void testGetOffsets() {
        val testData = Arrays.asList(
                new int[]{1},
                new int[]{1, 2, 3},
                new int[]{10, 20, 30},
                new int[]{16, 12, 12, 12, 12});
        for (val lengths : testData) {
            val hc = HashConfig.of(lengths);
            Assert.assertEquals("Unexpected result from getHashCount().", lengths.length, hc.getHashCount());
            val expectedMinLength = Arrays.stream(lengths).sum();
            Assert.assertEquals("Unexpected result from getMinHashLengthBytes().", expectedMinLength, hc.getMinHashLengthBytes());
            int expectedStartOffset = 0;
            for (int i = 0; i < lengths.length; i++) {
                int expectedEndOffset = expectedStartOffset + lengths[i];
                val o = hc.getOffsets(i);
                Assert.assertEquals("Unexpected start offset.", expectedStartOffset, (int) o.getLeft());
                Assert.assertEquals("Unexpected end offset.", expectedEndOffset, (int) o.getRight());
                expectedStartOffset = expectedEndOffset;
            }
        }
    }
}
