/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
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
package com.emc.pravega.common.segment;

import java.util.Stack;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import com.emc.pravega.testcommon.AssertExtensions;

/**
 * Unit tests for StreamSegmentNameUtils class.
 */
public class StreamSegmentNameUtilsTests {
    /**
     * Tests the basic batch name generation, with only one level of batches.
     */
    @Test
    public void testSimpleBatchNameGeneration() {
        int transactionCount = 100;
        String segmentName = "foo";
        String parentName = StreamSegmentNameUtils.getParentStreamSegmentName(segmentName);
        Assert.assertNull("getParentStreamSegmentName() extracted a parent name when none was expected.", parentName);

        for (int i = 0; i < transactionCount; i++) {
            String transactionName = StreamSegmentNameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
            AssertExtensions.assertNotNullOrEmpty("getTransactionNameFromId() did not return any Segment Name.", transactionName);
            AssertExtensions.assertGreaterThan("getTransactionNameFromId() returned a Segment Name that is shorter than the base.", segmentName.length(), transactionName.length());

            parentName = StreamSegmentNameUtils.getParentStreamSegmentName(transactionName);
            Assert.assertEquals("getParentStreamSegmentName() generated an unexpected value for parent.", segmentName, parentName);
        }
    }

    /**
     * Tests recursive batch generation. This is not a direct requirement or in any way represents how the service works,
     * but it is good to test the principles of batch generation (i.e., only look at the last part of a segment name and
     * ignore the first part).
     */
    @Test
    public void testRecursiveBatchNameGeneration() {
        int recursionCount = 10;
        Stack<String> names = new Stack<>();
        names.push("foo"); // Base segment.
        for (int i = 0; i < recursionCount; i++) {
            // Generate a batch name for the last generated name.
            names.push(StreamSegmentNameUtils.getTransactionNameFromId(names.peek(), UUID.randomUUID()));
        }

        // Make sure we can retrace our roots.
        String lastName = names.pop();
        while (names.size() > 0) {
            String expectedName = names.pop();
            String actualName = StreamSegmentNameUtils.getParentStreamSegmentName(lastName);
            Assert.assertEquals("Unexpected parent name.", expectedName, actualName);
            lastName = expectedName;
        }

        Assert.assertNull("Unexpected parent name when none was expected.", StreamSegmentNameUtils.getParentStreamSegmentName(lastName));
    }
}
