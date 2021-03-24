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
package io.pravega.shared.segment;

import io.pravega.shared.NameUtils;
import io.pravega.test.common.AssertExtensions;
import java.nio.CharBuffer;
import java.util.HashMap;
import java.util.UUID;
import java.util.function.Function;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for SegmentToContainerMapper.
 */
public class SegmentToContainerMapperTests {

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
        testUniformMapping(20, 10000, 0.10, num -> getSegmentName(num));
        testUniformMapping(20, 10000, 0.10, num -> Integer.toString(num));
        testUniformMapping(100, 100000, 0.10, num -> getSegmentName(num));
        testUniformMapping(100, 100000, 0.10, num -> Integer.toString(num));
        testUniformMapping(100, 100000, 0.10, num -> Integer.toBinaryString(num));
        testUniformMapping(100, 100000, 0.10, num -> Integer.toOctalString(num));
        testUniformMapping(100, 100000, 0.10, num -> Integer.toHexString(num));
    }

    private void testUniformMapping(int containerCount, int streamSegmentCount, double maxDeviation,
            Function<Integer, String> nameGen) {
        SegmentToContainerMapper m = new SegmentToContainerMapper(containerCount);
        assertEquals("Unexpected value for getTotalContainerCount().",
                            containerCount,
                            m.getTotalContainerCount());
        HashMap<Integer, Integer> containerMapCounts = new HashMap<>();

        // Generate all possible names with the given length and assign them to a container.
        for (int segmentId = 0; segmentId < streamSegmentCount; segmentId++) {
            String segmentName = nameGen.apply(segmentId);
            int containerId = m.getContainerId(segmentName);
            containerMapCounts.put(containerId, containerMapCounts.getOrDefault(containerId, 0) + 1);
        }
        assertEquals(containerCount, containerMapCounts.size());
        int target = streamSegmentCount / containerCount;
        // Count min and max number of assignments.
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (int count : containerMapCounts.values()) {
            min = Math.min(min, count);
            max = Math.max(max, count);
        }
        assertTrue(max >= target);
        assertTrue(min <= target);
        String msg = String.format("Too large of a variation between min and max mapping counts to containers. Min = %d, Max = %d.", min, max);
        AssertExtensions.assertLessThan(msg, (int) (maxDeviation * target),  max - target);
        AssertExtensions.assertLessThan(msg, (int) (maxDeviation * target), target - min);
    }

    /**
     * Tests that Transactions are mapped to the same container as their parents.
     */
    @Test
    public void testTransactionMapping() {
        int containerCount = 16;
        int streamSegmentCount = 256;
        int transactionPerParentCount = 10;

        SegmentToContainerMapper m = new SegmentToContainerMapper(containerCount);

        // Generate all possible names with the given length and assign them to a container.
        for (int segmentId = 0; segmentId < streamSegmentCount; segmentId++) {
            String segmentName = getSegmentName(segmentId);
            int containerId = m.getContainerId(segmentName);
            for (int i = 0; i < transactionPerParentCount; i++) {
                String transcationName = NameUtils.getTransactionNameFromId(segmentName, UUID.randomUUID());
                int transactionContainerId = m.getContainerId(transcationName);
                Assert.assertEquals("Parent and Transaction were not assigned to the same container.", containerId, transactionContainerId);
            }
        }
    }

    @Test
    public void testSegmentMapping() {
        int containerCount = 16;
        int streamSegmentCount = 256;
        int epochCount = 10;
        SegmentToContainerMapper m = new SegmentToContainerMapper(containerCount);

        // Generate all possible names with the given length and assign them to a container.
        for (int segmentId = 0; segmentId < streamSegmentCount; segmentId++) {
            String segmentName = NameUtils.getQualifiedStreamSegmentName("scope", "stream",
                    NameUtils.computeSegmentId(segmentId, 0));
            int containerId = m.getContainerId(segmentName);
            for (int i = 0; i < epochCount; i++) {
                String duplicate = NameUtils.getQualifiedStreamSegmentName("scope", "stream",
                        NameUtils.computeSegmentId(segmentId, i));
                int duplicateContainerId = m.getContainerId(duplicate);
                Assert.assertEquals("Parent and Transaction were not assigned to the same container.", containerId, duplicateContainerId);
            }
        }
    }

    private String getSegmentName(int segmentId) {
        CharBuffer buffer = CharBuffer.allocate(4);
        segmentId = Integer.reverseBytes(Integer.reverse(segmentId));
        while (segmentId != 0) {
            buffer.put((char) (segmentId & 0x00FF));
            segmentId = segmentId >>> 8;
        }
        buffer.flip();
        return new String(buffer.array());
    }
}
