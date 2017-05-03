/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.service.server;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the CacheKey class.
 */
public class CacheKeyTests {
    private static final int SEGMENT_COUNT = 1000;
    private static final int OFFSET_COUNT = 10000;
    private static final long OFFSET_MULTIPLIER = 1024 * 1024;

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
