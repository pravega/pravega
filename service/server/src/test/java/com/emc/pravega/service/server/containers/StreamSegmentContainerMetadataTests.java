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

import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.testcommon.AssertExtensions;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.function.Function;

/**
 * Unit tests for StreamSegmentContainerMetadata class.
 */
public class StreamSegmentContainerMetadataTests {
    private static final String CONTAINER_ID = "Container";
    private static final int SEGMENT_COUNT = 100;
    private static final int BATCHES_PER_SEGMENT_COUNT = 2;

    /**
     * Tests SequenceNumber-related operations.
     */
    @Test
    public void testSequenceNumber() {
        StreamSegmentContainerMetadata m = new StreamSegmentContainerMetadata(CONTAINER_ID);
        for (long expectedSeqNo = 1; expectedSeqNo < 100; expectedSeqNo++) {
            long actualSeqNo = m.nextOperationSequenceNumber();
            Assert.assertEquals("Unexpected result from nextOperationSequenceNumber.", expectedSeqNo, actualSeqNo);
        }

        AssertExtensions.assertThrows(
                "setOperationSequenceNumber allowed updating the sequence number in non-recovery mode.",
                () -> m.setOperationSequenceNumber(Integer.MAX_VALUE),
                ex -> ex instanceof IllegalStateException);

        // In recovery mode: setOperationSequenceNumber should work, nextOperationSequenceNumber should not.
        m.enterRecoveryMode();
        AssertExtensions.assertThrows(
                "setOperationSequenceNumber allowed updating the sequence number to a smaller value.",
                () -> m.setOperationSequenceNumber(1),
                ex -> ex instanceof IllegalArgumentException);

        m.setOperationSequenceNumber(Integer.MAX_VALUE);

        AssertExtensions.assertThrows(
                "nextOperationSequenceNumber worked in recovery mode.",
                m::nextOperationSequenceNumber,
                ex -> ex instanceof IllegalStateException);

        m.exitRecoveryMode();
        long actualSeqNo = m.getOperationSequenceNumber();
        Assert.assertEquals("Unexpected value from getNewSequenceNumber after setting the value.", Integer.MAX_VALUE, actualSeqNo);
    }

    /**
     * Tests the ability to map new StreamSegments (as well as batches).
     */
    @Test
    public void testMapStreamSegment() {
        StreamSegmentContainerMetadata m = new StreamSegmentContainerMetadata(CONTAINER_ID);
        ArrayList<Long> segmentIds = new ArrayList<>();
        for (long i = 0; i < SEGMENT_COUNT; i++) {
            final long segmentId = segmentIds.size();
            segmentIds.add(segmentId);
            String segmentName = getName(segmentId);

            // This should work.
            m.mapStreamSegmentId(segmentName, segmentId);
            Assert.assertEquals("Unexpected value from getStreamSegmentId (Stand-alone Segment).", segmentId, m.getStreamSegmentId(segmentName));

            // Now check that we cannot re-map the same SegmentId or SegmentName.
            AssertExtensions.assertThrows(
                    "mapStreamSegmentId allowed mapping the same SegmentId twice.",
                    () -> m.mapStreamSegmentId(segmentName + "foo", segmentId),
                    ex -> ex instanceof IllegalArgumentException);
            AssertExtensions.assertThrows(
                    "mapStreamSegmentId allowed mapping the same SegmentName twice.",
                    () -> m.mapStreamSegmentId(segmentName, segmentId + 1),
                    ex -> ex instanceof IllegalArgumentException);

            for (long j = 0; j < BATCHES_PER_SEGMENT_COUNT; j++) {
                final long batchId = segmentIds.size();
                segmentIds.add(batchId);
                String batchName = getName(batchId);

                AssertExtensions.assertThrows(
                        "mapStreamSegmentId allowed mapping the a batch to an inexistent parent.",
                        () -> m.mapStreamSegmentId(batchName, batchId, batchId),
                        ex -> ex instanceof IllegalArgumentException);

                // This should work.
                m.mapStreamSegmentId(batchName, batchId, segmentId);
                Assert.assertEquals("Unexpected value from getStreamSegmentId (batch Segment).", batchId, m.getStreamSegmentId(batchName));

                // Now check that we cannot re-map the same BatchId or Name.
                AssertExtensions.assertThrows(
                        "mapStreamSegmentId allowed mapping the same Batch SegmentId twice.",
                        () -> m.mapStreamSegmentId(batchName + "foo", batchId, segmentId),
                        ex -> ex instanceof IllegalArgumentException);
                AssertExtensions.assertThrows(
                        "mapStreamSegmentId allowed mapping the same Batch SegmentName twice.",
                        () -> m.mapStreamSegmentId(batchName, batchId + 1, segmentId),
                        ex -> ex instanceof IllegalArgumentException);

                // Now check that we cannot map a batch to another batch.
                AssertExtensions.assertThrows(
                        "mapStreamSegmentId allowed mapping the a batch to another batch.",
                        () -> m.mapStreamSegmentId(batchName + "foo", batchId + 1, batchId),
                        ex -> ex instanceof IllegalArgumentException);
            }
        }

        Collection<Long> metadataSegmentIds = m.getAllStreamSegmentIds();
        AssertExtensions.assertContainsSameElements("Metadata does not contain the expected Segment Ids", segmentIds, metadataSegmentIds);
    }

    /**
     * Tests the ability to delete a StreamSegment from the metadata, as well as any dependent (batch) StreamSegments.
     */
    @Test
    public void testDeleteStreamSegment() {
        StreamSegmentContainerMetadata m = new StreamSegmentContainerMetadata(CONTAINER_ID);
        ArrayList<Long> segmentIds = new ArrayList<>();
        for (long i = 0; i < SEGMENT_COUNT; i++) {
            final long segmentId = segmentIds.size();
            segmentIds.add(segmentId);
            m.mapStreamSegmentId(getName(segmentId), segmentId);
            for (long j = 0; j < BATCHES_PER_SEGMENT_COUNT; j++) {
                final long batchId = segmentIds.size();
                segmentIds.add(batchId);
                m.mapStreamSegmentId(getName(batchId), batchId, segmentId);
            }
        }

        // By construction (see above, any index i=3n is a parent StreamSegment, and any index i=3n+1 or 3n+2 is a batch).
        // Let's delete a few parent StreamSegments and verify their batches are also deleted.
        // Then delete only batches, and verify those are the only ones to be deleted.
        final int groupSize = BATCHES_PER_SEGMENT_COUNT + 1;
        ArrayList<Integer> streamSegmentsToDelete = new ArrayList<>();
        ArrayList<Integer> batchesToDelete = new ArrayList<>();
        for (int i = 0; i < segmentIds.size(); i++) {
            if (i < segmentIds.size() / 2) {
                // In the first half, we only delete the parents (which will force the batches to be deleted too).
                if (i % groupSize == 0) {
                    streamSegmentsToDelete.add(i);
                }
            } else {
                // In the second half, we only delete the first batch of any segment.
                if (i % groupSize == 1) {
                    batchesToDelete.add(i);
                }
            }
        }

        // Delete stand-alone StreamSegments (and verify batches are also deleted).
        Collection<Long> deletedStreamSegmentIds = new HashSet<>();
        for (int index : streamSegmentsToDelete) {
            long segmentId = segmentIds.get(index);
            String name = m.getStreamSegmentMetadata(segmentId).getName();
            Collection<String> expectedDeletedSegmentNames = new ArrayList<>();
            expectedDeletedSegmentNames.add(name);
            deletedStreamSegmentIds.add(segmentId);
            for (int batchIndex = 0; batchIndex < BATCHES_PER_SEGMENT_COUNT; batchIndex++) {
                long batchId = segmentIds.get(index + batchIndex + 1);
                deletedStreamSegmentIds.add(batchId);
                expectedDeletedSegmentNames.add(m.getStreamSegmentMetadata(batchId).getName());
            }

            Collection<String> deletedSegmentNames = m.deleteStreamSegment(name);
            AssertExtensions.assertContainsSameElements("Unexpected StreamSegments were deleted.", expectedDeletedSegmentNames, deletedSegmentNames);
        }

        // Delete batches.
        for (int index : batchesToDelete) {
            long batchId = segmentIds.get(index);
            String name = m.getStreamSegmentMetadata(batchId).getName();
            Collection<String> expectedDeletedSegmentNames = new ArrayList<>();
            deletedStreamSegmentIds.add(batchId);
            expectedDeletedSegmentNames.add(name);

            Collection<String> deletedSegmentNames = m.deleteStreamSegment(name);
            AssertExtensions.assertContainsSameElements("Unexpected StreamSegments were deleted.", expectedDeletedSegmentNames, deletedSegmentNames);
        }

        // Verify deleted segments have not been actually removed from the metadata.
        Collection<Long> metadataSegmentIds = m.getAllStreamSegmentIds();
        AssertExtensions.assertContainsSameElements("Metadata does not contain the expected Segment Ids", segmentIds, metadataSegmentIds);

        // Verify individual StreamSegmentMetadata.
        for (long segmentId : segmentIds) {
            boolean expectDeleted = deletedStreamSegmentIds.contains(segmentId);
            Assert.assertEquals("Unexpected value for isDeleted.", expectDeleted, m.getStreamSegmentMetadata(segmentId).isDeleted());
        }
    }

    /**
     * Tests the ability for the metadata to reset itself.
     */
    @Test
    public void testReset() {
        // Segments, Sequence Number + Truncation markers
        StreamSegmentContainerMetadata m = new StreamSegmentContainerMetadata(CONTAINER_ID);

        // Set a high Sequence Number
        m.enterRecoveryMode();
        m.setOperationSequenceNumber(Integer.MAX_VALUE);
        m.exitRecoveryMode();

        // Populate some StreamSegments.
        ArrayList<Long> segmentIds = new ArrayList<>();
        for (long i = 0; i < SEGMENT_COUNT; i++) {
            final long segmentId = segmentIds.size();
            segmentIds.add(segmentId);
            m.mapStreamSegmentId(getName(segmentId), segmentId);
            for (long j = 0; j < BATCHES_PER_SEGMENT_COUNT; j++) {
                final long batchId = segmentIds.size();
                segmentIds.add(batchId);
                m.mapStreamSegmentId(getName(batchId), batchId, segmentId);
            }
        }

        // Add some truncation markers.
        final long truncationMarkerSeqNo = 10;
        m.recordTruncationMarker(truncationMarkerSeqNo, truncationMarkerSeqNo);
        m.setValidTruncationPoint(truncationMarkerSeqNo);

        AssertExtensions.assertThrows(
                "reset() worked in non-recovery mode.",
                m::reset,
                ex -> ex instanceof IllegalStateException);

        // Do the reset.
        m.enterRecoveryMode();
        m.reset();
        m.exitRecoveryMode();

        // Verify everything was reset.
        Assert.assertEquals("Sequence Number was not reset.", ContainerMetadata.INITIAL_OPERATION_SEQUENCE_NUMBER, m.getOperationSequenceNumber());
        for (long segmentId : segmentIds) {
            Assert.assertEquals("SegmentMetadata was not reset (getStreamSegmentId).", ContainerMetadata.NO_STREAM_SEGMENT_ID, m.getStreamSegmentId(getName(segmentId)));
            Assert.assertNull("SegmentMetadata was not reset (getStreamSegmentMetadata).", m.getStreamSegmentMetadata(segmentId));
        }

        long tmSeqNo = m.getClosestTruncationMarker(truncationMarkerSeqNo);
        AssertExtensions.assertLessThan("Truncation Markers were not reset.", 0, tmSeqNo);
        Assert.assertFalse("Truncation Points were not reset.", m.isValidTruncationPoint(tmSeqNo));
    }

    /**
     * Tests the Truncation Marker functionality (truncation points is tested separately).
     */
    @Test
    public void testTruncationMarkers() {
        final long maxSeqNo = 1000;
        final int markerFrequency = 13;
        Function<Long, Long> getDataFrameSequence = seqNo -> Integer.MAX_VALUE + seqNo * seqNo;
        StreamSegmentContainerMetadata m = new StreamSegmentContainerMetadata(CONTAINER_ID);

        // Record some truncation markers, starting a few steps after initial.
        for (long seqNo = markerFrequency; seqNo <= maxSeqNo; seqNo += markerFrequency) {
            m.recordTruncationMarker(seqNo, getDataFrameSequence.apply(seqNo));
        }

        // Verify them.
        for (long seqNo = 0; seqNo < maxSeqNo + markerFrequency; seqNo++) {
            long expectedTruncationMarker = -1;
            if (seqNo >= markerFrequency) {
                long input = seqNo > maxSeqNo ? maxSeqNo : seqNo;
                expectedTruncationMarker = getDataFrameSequence.apply(input - input % markerFrequency);
            }

            long truncationMarker = m.getClosestTruncationMarker(seqNo);
            Assert.assertEquals("Unexpected truncation marker value for Op Sequence Number " + seqNo, expectedTruncationMarker, truncationMarker);
        }

        // Remove some truncation markers & verify again.
        for (long seqNo = 0; seqNo < maxSeqNo + markerFrequency; seqNo++) {
            m.removeTruncationMarkers(seqNo);

            // Check that the removal actually made sense (it should return -1 now).
            long expectedTruncationMarker = -1;
            long truncationMarker = m.getClosestTruncationMarker(seqNo);
            Assert.assertEquals("Unexpected truncation marker value after removal for Op Sequence Number " + seqNo, expectedTruncationMarker, truncationMarker);

            // Check that the next higher up still works.
            long input = seqNo + markerFrequency;
            input = input > maxSeqNo ? maxSeqNo : input;
            if (seqNo > maxSeqNo - markerFrequency) {
                // We have already removed all possible truncation markers, so expect the result to be -1.
                expectedTruncationMarker = -1;
            } else {
                expectedTruncationMarker = getDataFrameSequence.apply(input - input % markerFrequency);
            }

            truncationMarker = m.getClosestTruncationMarker(input);
            Assert.assertEquals("Unexpected truncation marker value for Op Sequence Number " + input + " after removing marker at Sequence Number " + seqNo, expectedTruncationMarker, truncationMarker);
        }
    }

    /**
     * Tests the ability to set and retrieve truncation points (truncation markers is tested separately).
     */
    @Test
    public void testValidTruncationPoints() {
        StreamSegmentContainerMetadata m = new StreamSegmentContainerMetadata(CONTAINER_ID);
        for (int i = 0; i < 100; i += 2) {
            m.setValidTruncationPoint(i);
        }

        for (int i = 0; i < 100; i++) {
            boolean expectedValid = i % 2 == 0;
            Assert.assertEquals("Unexpected result from isValidTruncationPoint.", expectedValid, m.isValidTruncationPoint(i));
        }
    }

    private String getName(long id) {
        return "Segment" + id;
    }
}
