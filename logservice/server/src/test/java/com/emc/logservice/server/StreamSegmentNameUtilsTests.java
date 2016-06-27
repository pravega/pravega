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

package com.emc.logservice.server;

import com.emc.nautilus.testcommon.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Stack;

/**
 * Unit tests for StreamSegmentNameUtils class.
 */
public class StreamSegmentNameUtilsTests {
    /**
     * Tests the basic batch name generation, with only one level of batches.
     */
    @Test
    public void testSimpleBatchNameGeneration() {
        int batchCount = 100;
        String segmentName = "foo";
        String parentName = StreamSegmentNameUtils.getParentStreamSegmentName(segmentName);
        Assert.assertNull("getParentStreamSegmentName() extracted a parent name when none was expected.", parentName);

        for (int i = 0; i < batchCount; i++) {
            String batchName = StreamSegmentNameUtils.generateBatchStreamSegmentName(segmentName);
            AssertExtensions.assertNotNullOrEmpty("generateBatchStreamSegmentName() did not generate any Segment Name.", batchName);
            AssertExtensions.assertGreaterThan("generateBatchStreamSegmentName() generated a Segment Name that is shorter than the base.", segmentName.length(), batchName.length());
            parentName = StreamSegmentNameUtils.getParentStreamSegmentName(batchName);
            Assert.assertEquals("getParentStreamSegmentName() generated an unexpected value for parent.", segmentName, parentName);

            // Alter a character (at a time) from the batch name and verify that it cannot derive a valid parent from it anymore.
            for (int j = 0; j < batchName.length(); j++) {
                String firstPart = j == 0 ? "" : batchName.substring(0, j - 1);
                String lastPart = j == batchName.length() - 1 ? "" : batchName.substring(j + 1);
                String badBatchName = String.format("%s%s%s", firstPart, (char) (batchName.charAt(j) + 1), lastPart);
                parentName = StreamSegmentNameUtils.getParentStreamSegmentName(badBatchName);
                Assert.assertNull("getParentStreamSegmentName() generated a value for parent when none was expected.", parentName);
            }
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
            names.push(StreamSegmentNameUtils.generateBatchStreamSegmentName(names.peek()));
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
