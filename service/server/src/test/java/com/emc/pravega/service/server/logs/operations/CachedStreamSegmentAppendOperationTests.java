/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.server.logs.operations;

import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.testcommon.AssertExtensions;
import java.util.UUID;
import org.junit.Assert;
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
        AppendContext context = new AppendContext(UUID.randomUUID(), 1);
        StreamSegmentAppendOperation baseOp = new StreamSegmentAppendOperation(SEGMENT_ID, data, context);
        baseOp.setSequenceNumber(1);
        baseOp.setStreamSegmentOffset(OFFSET);

        // Valid scenarios.
        CachedStreamSegmentAppendOperation newOp = new CachedStreamSegmentAppendOperation(baseOp);
        Assert.assertEquals("Unexpected sequence number.", baseOp.getSequenceNumber(), newOp.getSequenceNumber());
        Assert.assertEquals("Unexpected offset.", baseOp.getStreamSegmentOffset(), newOp.getStreamSegmentOffset());
        Assert.assertEquals("Unexpected length .", baseOp.getData().length, newOp.getLength());

        // Invalid scenarios.
        AssertExtensions.assertThrows(
                "Unexpected exception when invalid offset.",
                () -> new CachedStreamSegmentAppendOperation(new StreamSegmentAppendOperation(SEGMENT_ID, data, context)),
                ex -> ex instanceof IllegalArgumentException || ex instanceof IllegalStateException);

        AssertExtensions.assertThrows(
                "Unexpected exception when invalid sequence number.",
                () -> {
                    StreamSegmentAppendOperation badOp = new StreamSegmentAppendOperation(SEGMENT_ID, data, context);
                    baseOp.setStreamSegmentOffset(OFFSET);
                    new CachedStreamSegmentAppendOperation(badOp);
                },
                ex -> ex instanceof IllegalArgumentException || ex instanceof IllegalStateException);
    }
}
