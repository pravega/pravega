package com.emc.logservice.server;

import com.emc.nautilus.testcommon.AssertExtensions;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

/**
 * Unit tests for SegmentToContainerMapper.
 */
public class SegmentToContainerMapperTests {
    private final static int MaxByte = 256;

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
        // DO not make this any larger. This will very easily grow out of proportions (MaxByte ^ streamSegmentNameByteCount).
        int streamSegmentNameByteCount = 2;

        // Calculate how many segments we have.
        int streamSegmentCount = (int) Math.pow(MaxByte, streamSegmentNameByteCount);

        // This is how much deviation we allow between min and max (container assignments).
        double maxDeviation = 0.01;

        SegmentToContainerMapper m = new SegmentToContainerMapper(containerCount);
        Assert.assertEquals("Unexpected value for getTotalContainerCount().", containerCount, m.getTotalContainerCount());
        HashMap<String, Integer> containerMapCounts = new HashMap<>();

        // Generate all possible names with the given length and assign them to a container.
        for (int segmentId = 0; segmentId < streamSegmentCount; segmentId++) {
            String segmentName = getSegmentName(segmentId, streamSegmentNameByteCount);
            String containerId = m.getContainerId(segmentName);
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
     * Tests that batches are mapped to the same container as their parents.
     */
    @Test
    public void testBatchMapping() {
        int containerCount = 16;
        int streamSegmentNameByteCount = 1;
        int streamSegmentCount = (int) Math.pow(MaxByte, streamSegmentNameByteCount);
        int batchPerParentCount = 10;

        SegmentToContainerMapper m = new SegmentToContainerMapper(containerCount);

        // Generate all possible names with the given length and assign them to a container.
        for (int segmentId = 0; segmentId < streamSegmentCount; segmentId++) {
            String segmentName = getSegmentName(segmentId, streamSegmentNameByteCount);
            String containerId = m.getContainerId(segmentName);
            for (int i = 0; i < batchPerParentCount; i++) {
                String batchName = StreamSegmentNameUtils.generateBatchStreamSegmentName(segmentName);
                String batchContainerId = m.getContainerId(batchName);
                Assert.assertEquals("Parent and batch were not assigned to the same container.", containerId, batchContainerId);
            }
        }
    }

    private String getSegmentName(int segmentId, int length) {
        char[] stringContents = new char[length];
        for (int i = 0; i < length; i++) {
            stringContents[i] = (char) (segmentId % MaxByte);
            segmentId /= MaxByte;
        }

        return new String(stringContents);
    }
}
