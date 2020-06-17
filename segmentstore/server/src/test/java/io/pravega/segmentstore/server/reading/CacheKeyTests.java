/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.reading;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the CacheKey class.
 */
public class CacheKeyTests {
    private static final int SEGMENT_COUNT = 1000;
    private static final int OFFSET_COUNT = 10000;
    private static final long OFFSET_MULTIPLIER = 1024 * 1024;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    /**
     * Tests the Serialization of CacheKey.
     */
    @Test
    public void testSerialization() {
        CacheKey lastKey = null;
        for (int segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
            for (long baseOffset = 0; baseOffset < OFFSET_COUNT; baseOffset += 1) {
                long offset = baseOffset * OFFSET_MULTIPLIER;
                CacheKey originalKey = new CacheKey(segmentId, offset);
                CacheKey newKey = new CacheKey(originalKey.serialize());

                Assert.assertTrue("equals() did not return true for equivalent keys.", originalKey.equals(newKey));
                Assert.assertEquals("hashCode() did not return the same value for equivalent keys.", originalKey.hashCode(), newKey.hashCode());
                Assert.assertEquals("getStreamSegmentId() did not return the same value for equivalent keys.", originalKey.getStreamSegmentId(), newKey.getStreamSegmentId());
                Assert.assertEquals("getOffset() did not return the same value for equivalent keys.", originalKey.getOffset(), newKey.getOffset());

                if (lastKey != null) {
                    Assert.assertFalse("equals() did not return false for different keys.", originalKey.equals(lastKey));
                }

                lastKey = originalKey;
            }
        }
    }
}
