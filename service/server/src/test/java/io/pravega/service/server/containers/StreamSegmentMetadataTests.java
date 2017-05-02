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

package io.pravega.service.server.containers;

import io.pravega.common.util.ImmutableDate;
import io.pravega.service.server.SegmentMetadata;
import io.pravega.service.server.SegmentMetadataComparer;
import io.pravega.test.common.AssertExtensions;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for StreamSegmentMetadata class.
 */
public class StreamSegmentMetadataTests {
    private static final String SEGMENT_NAME = "Segment";
    private static final long SEGMENT_ID = 1;
    private static final long PARENT_SEGMENT_ID = 2;
    private static final int CONTAINER_ID = 1234567;
    private static final int ATTRIBUTE_COUNT = 100;

    /**
     * Tests that Attributes are properly recorded and updated
     */
    @Test
    public void testAttributes() {
        StreamSegmentMetadata metadata = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, PARENT_SEGMENT_ID, CONTAINER_ID);

        // Step 1: initial set of attributes.
        Random rnd = new Random(0);
        val expectedAttributes = generateAttributes(rnd);

        metadata.updateAttributes(expectedAttributes);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after initial set.", expectedAttributes, metadata);

        // Step 2: Update half of attributes and add 50% more.
        int count = 0;
        val keyIterator = expectedAttributes.keySet().iterator();
        val attributeUpdates = new HashMap<UUID, Long>();

        // Update
        while (count < ATTRIBUTE_COUNT / 2 && keyIterator.hasNext()) {
            attributeUpdates.put(keyIterator.next(), rnd.nextLong());
            count++;
        }

        // Now add a few more.
        while (attributeUpdates.size() < ATTRIBUTE_COUNT) {
            attributeUpdates.put(UUID.randomUUID(), rnd.nextLong());
        }

        attributeUpdates.forEach(expectedAttributes::put);
        metadata.updateAttributes(attributeUpdates);
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after update.", expectedAttributes, metadata);

        // Step 3: Remove all attributes.
        val attributesToRemove = new HashMap<UUID, Long>();
        expectedAttributes.forEach((id, value) -> attributesToRemove.put(id, SegmentMetadata.NULL_ATTRIBUTE_VALUE));
        metadata.updateAttributes(attributesToRemove);
        expectedAttributes.clear();
        SegmentMetadataComparer.assertSameAttributes("Unexpected attributes after removal.", expectedAttributes, metadata);
    }

    /**
     * Tests the copyFrom() method.
     */
    @Test
    public void testCopyFrom() {
        StreamSegmentMetadata baseMetadata = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, PARENT_SEGMENT_ID, CONTAINER_ID);
        baseMetadata.updateAttributes(generateAttributes(new Random(0)));
        baseMetadata.setStorageLength(1233);
        baseMetadata.setDurableLogLength(3235342);
        baseMetadata.setLastModified(new ImmutableDate());
        baseMetadata.markDeleted();
        baseMetadata.markSealed();
        baseMetadata.markMerged();
        baseMetadata.setLastUsed(1545895);

        // Normal metadata copy.
        StreamSegmentMetadata newMetadata = new StreamSegmentMetadata(SEGMENT_NAME, SEGMENT_ID, PARENT_SEGMENT_ID, CONTAINER_ID);
        newMetadata.copyFrom(baseMetadata);
        SegmentMetadataComparer.assertEquals("Metadata copy:", baseMetadata, newMetadata);
        Assert.assertEquals("Metadata copy: getLastUsed differs.",
                baseMetadata.getLastUsed(), newMetadata.getLastUsed());

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

    private Map<UUID, Long> generateAttributes(Random rnd) {
        val result = new HashMap<UUID, Long>();
        for (int i = 0; i < ATTRIBUTE_COUNT; i++) {
            result.put(UUID.randomUUID(), rnd.nextLong());
        }

        return result;
    }
}
