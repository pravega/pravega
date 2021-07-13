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
package io.pravega.segmentstore.server.logs.operations;

import io.pravega.common.util.ByteArraySegment;
import io.pravega.test.common.AssertExtensions;
import lombok.val;
import org.junit.Test;

/**
 * Unit tests for CachedStreamSegmentAppendOperation.
 */
public class CachedStreamSegmentAppendOperationTests {
    private static final long SEGMENT_ID = 1234;
    private static final long OFFSET = 123456789;

    /**
     * Tests the constructor of the operation, based on an existing StreamSegmentAppendOperation and CacheKey.
     */
    @Test
    public void testConstructor() {
        ByteArraySegment data = new ByteArraySegment("foo".getBytes());
        val attributes = StreamSegmentAppendOperationTests.createAttributes();
        StreamSegmentAppendOperation baseOp = new StreamSegmentAppendOperation(SEGMENT_ID, data, attributes);
        baseOp.setSequenceNumber(1);
        baseOp.setStreamSegmentOffset(OFFSET);

        // Valid scenarios.
        CachedStreamSegmentAppendOperation newOp = new CachedStreamSegmentAppendOperation(baseOp);
        OperationComparer.DEFAULT.assertEquals("Unexpected result from constructor.", baseOp, newOp);

        // Invalid scenarios.
        AssertExtensions.assertThrows(
                "Unexpected exception when invalid offset.",
                () -> new CachedStreamSegmentAppendOperation(new StreamSegmentAppendOperation(SEGMENT_ID, data, attributes)),
                ex -> ex instanceof IllegalArgumentException || ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "Unexpected exception when invalid sequence number.",
                () -> {
                    StreamSegmentAppendOperation badOp = new StreamSegmentAppendOperation(SEGMENT_ID, data, attributes);
                    baseOp.setStreamSegmentOffset(OFFSET);
                    new CachedStreamSegmentAppendOperation(badOp);
                },
                ex -> ex instanceof IllegalArgumentException || ex instanceof IllegalStateException);
    }
}
