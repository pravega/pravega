/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.tables;

import com.google.common.collect.ImmutableMap;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the CacheBucketOffset class.
 */
public class CacheBucketOffsetTests {
    /**
     * Tests the ability to encode and decode CacheBucketOffset instances.
     */
    @Test
    public void testEncodeDecode() {
        val testData = ImmutableMap
                .<CacheBucketOffset, Long>builder()
                .put(new CacheBucketOffset(Long.MAX_VALUE, true), -1L)
                .put(new CacheBucketOffset(Long.MAX_VALUE, false), Long.MAX_VALUE)
                .put(new CacheBucketOffset(0L, true), Long.MIN_VALUE)
                .put(new CacheBucketOffset(0L, false), 0L)
                .put(new CacheBucketOffset(1234L, true), 1234 | Long.MIN_VALUE)
                .put(new CacheBucketOffset(1234L, false), 1234L)
                .build();
        for (val e : testData.entrySet()) {
            CacheBucketOffset expectedOffset = e.getKey();
            long expectedEncodedValue = e.getValue();
            long encodedValue = expectedOffset.encode();
            Assert.assertEquals("Unexpected encoded value for " + expectedOffset, expectedEncodedValue, encodedValue);
            CacheBucketOffset decodedOffset = CacheBucketOffset.decode(encodedValue);
            Assert.assertEquals("Unexpected decoded value for " + expectedEncodedValue, expectedOffset, decodedOffset);
        }
    }
}
