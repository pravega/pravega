/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.service.server.logs.operations;

import io.pravega.testcommon.AssertExtensions;
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
        byte[] data = "foo".getBytes();
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
