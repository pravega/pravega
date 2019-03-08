/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.attributes;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for the CacheKey class.
 */
public class CacheKeyTests {
    private static final int SEGMENT_COUNT = 100;
    private static final int ENTRY_COUNT = 128;
    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    /**
     * Tests the Serialization of CacheKey.
     */
    @Test
    public void testSerialization() {
        CacheKey lastKey = null;
        for (int segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
            for (int entryId = 0; entryId < ENTRY_COUNT; entryId += 1) {
                CacheKey originalKey = new CacheKey(segmentId, entryId);
                CacheKey newKey = new CacheKey(originalKey.serialize());

                Assert.assertEquals("equals() did not return true for equivalent keys.", originalKey, newKey);
                Assert.assertEquals("hashCode() did not return the same value for equivalent keys.", originalKey.hashCode(), newKey.hashCode());
                Assert.assertEquals("getStreamSegmentId() did not return the same value for equivalent keys.",
                        originalKey.getSegmentId(), newKey.getSegmentId());
                Assert.assertEquals("getOffset() did not return the same value for equivalent keys.",
                        originalKey.getOffset(), newKey.getOffset());

                if (lastKey != null) {
                    Assert.assertNotEquals("equals() did not return false for different keys.", originalKey, lastKey);
                }

                lastKey = originalKey;
            }
        }
    }
}
