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

package com.emc.pravega.service.server;

import org.junit.Assert;
import org.junit.Test;

import com.emc.pravega.testcommon.AssertExtensions;

import java.util.HashMap;
import java.util.UUID;

/**
 * Unit tests for SegmentToContainerMapper.
 */
public class SegmentToContainerMapperTests {
    private final static int MAX_BYTE = 256;

    /**
     * Tests that the constructor only allows valid configurations.
     */
    @Test
    public void testConstructor() {
        AssertExtensions.assertThrows(
                "SegmentToContainerManager could be created with no containers.",
                () -> new SegmentToContainerMapper(0),
                ex -> ex instanceof IllegalArgumentException);
    }

    /**
     * Tests that, given a uniform StreamSegment name distribution, SegmentToContainerManager uniformly assigns those
     * Segment Names to containers.
     */
    @Test
    public void testUniformMapping() {
        // For our test, 128 containers will do.
        int containerCount = 128;

        // To generate names uniformly, we will generate any possible Name with this length.
        // DO not make this any larger. This will very easily grow out of proportions (MAX_BYTE ^ streamSegmentNameByteCount).
        int streamSegmentNameByteCount = 2;

        // Calculate how many segments we have.
        int streamSegmentCount = (int) Math.pow(MAX_BYTE, streamSegmentNameByteCount);

        // This is how much deviation we allow between min and max (container assignments).
        double maxDeviation = 0.01;

        SegmentToContainerMapper m = new SegmentToContainerMapper(containerCount);
        Assert.assertEquals("Unexpected value for getTotalContainerCount().", containerCount, m.getTotalContainerCount());
        HashMap<Integer, Integer> containerMapCounts = new HashMap<>();

        // Generate all possible names with the given length and assign them to a container.
        for (int segmentId = 0; segmentId < streamSegmentCount; segmentId++) {
            String segmentName = getSegmentName(segmentId, streamSegmentNameByteCount);
            int containerId = m.getContainerId(segmentName);
            containerMapCounts.put(containerId, containerMapCounts.getOrDefault(containerId, 0) + 1);
        }

        // Count min and max number of assignments.
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int count : containerMapCounts.values()) {
            min = Math.min(min, count);
            max = Math.max(max, count);
        }

        // Verify that min and max do not deviate too much from each other.
        AssertExtensions.assertGreaterThan(
                String.format("Too large of a variation between min and max mapping counts to containers. Min = %d, Max = %d.", min, max),
                max - min,
                (int) (maxDeviation * max));
    }

    /**
     * Tests that Transactions are mapped to the same container as their parents.
     */
    @Test
    public void testTransactionMapping() {
        int containerCount = 16;
        int streamSegmentNameByteCount = 1;
        int streamSegmentCount = (int) Math.pow(MAX_BYTE, streamSegmentNameByteCount);
        int transactionPerParentCount = 10;

        SegmentToContainerMapper m = new SegmentToContainerMapper(containerCount);

        // Generate all possible names with the given length and assign them to a container.
        for (int segmentId = 0; segmentId < streamSegmentCount; segmentId++) {
            String segmentName = getSegmentName(segmentId, streamSegmentNameByteCount);
            int containerId = m.getContainerId(segmentName);
            for (int i = 0; i < transactionPerParentCount; i++) {
                String transcationName = StreamSegmentNameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
                int transactionContainerId = m.getContainerId(transcationName);
                Assert.assertEquals("Parent and Transaction were not assigned to the same container.", containerId, transactionContainerId);
            }
        }
    }

    private String getSegmentName(int segmentId, int length) {
        char[] stringContents = new char[length];
        for (int i = 0; i < length; i++) {
            stringContents[i] = (char) (segmentId % MAX_BYTE);
            segmentId /= MAX_BYTE;
        }

        return new String(stringContents);
    }
}
