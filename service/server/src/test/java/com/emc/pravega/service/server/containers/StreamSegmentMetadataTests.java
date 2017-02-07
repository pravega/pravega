/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.containers;

import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.testcommon.AssertExtensions;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;

/**
 * Unit tests for StreamSegmentMetadata class.
 */
public class StreamSegmentMetadataTests {
    private static final String SEGMENT_NAME = "Segment";
    private static final long SEGMENT_ID = 1;
    private static final long PARENT_SEGMENT_ID = 2;
    private static final int CONTAINER_ID = 1234567;

    /**
     * Tests the fact that AppendContexts are properly recorded and returned, when queried.
     */
    @Test
    public void testAppendContexts() {
        final int clientCount = 20;
        final int contextsPerClient = 10;
        HashMap<UUID, AppendContext> lastContexts = new HashMap<>();
        for (int i = 0; i < clientCount; i++) {
            lastContexts.put(UUID.randomUUID(), null);
        }

        StreamSegmentMetadata m = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, PARENT_SEGMENT_ID, CONTAINER_ID);
        for (int i = 0; i < contextsPerClient; i++) {
            for (UUID clientId : lastContexts.keySet()) {
                AppendContext c = new AppendContext(clientId, i);
                lastContexts.put(clientId, c);
                m.recordAppendContext(c);
            }

            for (UUID clientId : lastContexts.keySet()) {
                AppendContext expected = lastContexts.get(clientId);
                AppendContext actual = m.getLastAppendContext(clientId);
                Assert.assertEquals("Unexpected AppendContext", expected, actual);
            }
        }

        Collection<UUID> recordedClients = m.getKnownClientIds();
        AssertExtensions.assertContainsSameElements("Unexpected collection of Client Ids", lastContexts.keySet(), recordedClients);
    }

    /**
     * Tests the copyFrom() method.
     */
    @Test
    public void testCopyFrom() {
        StreamSegmentMetadata baseMetadata = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, PARENT_SEGMENT_ID, CONTAINER_ID);
        final int clientCount = 20;
        for (int i = 0; i < clientCount; i++) {
            baseMetadata.recordAppendContext(new AppendContext(UUID.randomUUID(), 1));
        }
        baseMetadata.setStorageLength(1233);
        baseMetadata.setDurableLogLength(3235342);
        baseMetadata.setLastModified(new Date());
        baseMetadata.markDeleted();
        baseMetadata.markSealed();
        baseMetadata.markMerged();

        // Normal metadata copy.
        StreamSegmentMetadata newMetadata = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, PARENT_SEGMENT_ID, CONTAINER_ID);
        newMetadata.copyFrom(baseMetadata);
        assertEquals("Normal metadata copy:", baseMetadata, newMetadata);

        // Verify we cannot copy from different StreamSegments.
        AssertExtensions.assertThrows(
                "copyFrom allowed copying from a metadata with a different Segment Name",
                () -> new StreamSegmentMetadata("foo", SEGMENT_ID, PARENT_SEGMENT_ID, CONTAINER_ID).copyFrom(baseMetadata),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "copyFrom allowed copying from a metadata with a different Segment Id",
                () -> new StreamSegmentMetadata(SEGMENT_NAME, -SEGMENT_ID, PARENT_SEGMENT_ID, CONTAINER_ID).copyFrom(baseMetadata),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows(
                "copyFrom allowed copying from a metadata with a different Parent Id",
                () -> new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, -PARENT_SEGMENT_ID, CONTAINER_ID).copyFrom(baseMetadata),
                ex -> ex instanceof IllegalArgumentException);
    }

    private static void assertEquals(String message, SegmentMetadata expected, SegmentMetadata actual) {
        Assert.assertEquals(message + " StorageLength differs.", expected.getStorageLength(), actual.getStorageLength());
        Assert.assertEquals(message + " DurableLogLength differs.", expected.getDurableLogLength(), actual.getDurableLogLength());
        Assert.assertEquals(message + " isDeleted differs.", expected.isDeleted(), actual.isDeleted());
        Assert.assertEquals(message + " isSealed differs.", expected.isSealed(), actual.isSealed());
        Assert.assertEquals(message + " isMerged differs.", expected.isMerged(), actual.isMerged());
        AssertExtensions.assertContainsSameElements(message + " KnownClientIds differ.", expected.getKnownClientIds(), actual.getKnownClientIds());
        for (UUID clientId : expected.getKnownClientIds()) {
            AppendContext expectedContext = expected.getLastAppendContext(clientId);
            AppendContext actualContext = actual.getLastAppendContext(clientId);
            Assert.assertEquals(message + " getLastAppendContext differs.", expectedContext, actualContext);
        }
    }
}
