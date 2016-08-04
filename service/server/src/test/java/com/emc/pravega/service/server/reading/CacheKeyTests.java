package com.emc.pravega.service.server.reading;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the CacheKey class.
 */
public class CacheKeyTests {
    private static final int CONTAINER_COUNT = 100;
    private static final int SEGMENT_COUNT = 100;
    private static final int OFFSET_COUNT = 1000;
    private static final long OFFSET_MULTIPLIER = 1024 * 1024;

    /**
     * Tests the Serialization of CacheKey.
     */
    @Test
    public void testSerialization() {
        CacheKey lastKey = null;
        for (int containerId = 0; containerId < CONTAINER_COUNT; containerId++) {
            for (int segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
                for (long baseOffset = 0; baseOffset < OFFSET_COUNT; baseOffset += 1) {
                    long offset = baseOffset * OFFSET_MULTIPLIER;
                    CacheKey originalKey = new CacheKey(containerId, segmentId, offset);
                    CacheKey newKey = new CacheKey(originalKey.getSerialization());

                    Assert.assertTrue("equals() did not return true for equivalent keys.", originalKey.equals(newKey));
                    Assert.assertEquals("hashCode() did not return the same value for equivalent keys.", originalKey.hashCode(), newKey.hashCode());

                    if (lastKey != null) {
                        Assert.assertFalse("equals() did not return false for different keys.", originalKey.equals(lastKey));
                    }
                    lastKey = originalKey;
                }
            }
        }
    }
}
