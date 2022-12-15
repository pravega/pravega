/**
 * Copyright Pravega Authors.
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
package io.pravega.segmentstore.contracts;

import io.pravega.test.common.AssertExtensions;
import org.junit.Test;

/**
 * Tests for {@link SegmentAdminApi}
 */
public class SegmentAdminApiTest {

    /**
     * Sanity test to check if a new chunk was created with the provided content.
     * @throws Exception Throws exception in case of any error.
     */
    @Test
    public void checkChunkStorageSanityThrows() throws Exception {
        MockSegmentAdminApi mockSegmentAdminApi = new MockSegmentAdminApi();
        AssertExtensions.assertThrows("checkChunkStorageSanity is not supported",
                () -> mockSegmentAdminApi.checkChunkStorageSanity(10, "TestChunk", 5, null),
                ex -> ex instanceof UnsupportedOperationException);
    }

    @Test
    public void evictMetaDataCacheThrows() throws Exception {
        MockSegmentAdminApi mockSegmentAdminApi = new MockSegmentAdminApi();
        AssertExtensions.assertThrows("evictMetaDataCache is not supported",
                () -> mockSegmentAdminApi.evictMetaDataCache(10, null),
                ex -> ex instanceof UnsupportedOperationException);
    }

    @Test
    public void evictReadIndexCacheThrows() throws Exception {
        MockSegmentAdminApi mockSegmentAdminApi = new MockSegmentAdminApi();
        AssertExtensions.assertThrows("evictReadIndexCache is not supported",
                () -> mockSegmentAdminApi.evictReadIndexCache(10, null),
                ex -> ex instanceof UnsupportedOperationException);
    }

    @Test
    public void evictReadIndexCacheForSegmentThrows() throws Exception {
        MockSegmentAdminApi mockSegmentAdminApi = new MockSegmentAdminApi();
        AssertExtensions.assertThrows("evictReadIndexCacheForSegment is not supported",
                () -> mockSegmentAdminApi.evictReadIndexCacheForSegment(10, "TestChunk", null),
                ex -> ex instanceof UnsupportedOperationException);
    }

    static class MockSegmentAdminApi implements SegmentAdminApi {
    }
}
