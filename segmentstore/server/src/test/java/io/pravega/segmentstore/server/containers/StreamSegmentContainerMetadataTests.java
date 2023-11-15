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
package io.pravega.segmentstore.server.containers;

import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.MetadataBuilder;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableContainerMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import io.pravega.segmentstore.storage.LogAddress;
import io.pravega.test.common.AssertExtensions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Unit tests for StreamSegmentContainerMetadata class.
 */
public class StreamSegmentContainerMetadataTests {
    private static final int CONTAINER_ID = 1234567;
    private static final int SEGMENT_COUNT = 100;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    /**
     * Tests SequenceNumber-related operations.
     */
    @Test
    public void testSequenceNumber() {
        final UpdateableContainerMetadata m = new MetadataBuilder(CONTAINER_ID).build();
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
     * Tests Epoch-related operations.
     */
    @Test
    public void testEpoch() {
        final int epoch = 10;
        final UpdateableContainerMetadata m = new MetadataBuilder(CONTAINER_ID).build();

        AssertExtensions.assertThrows(
                "setContainerEpoch allowed updating the sequence number in non-recovery mode.",
                () -> m.setContainerEpoch(Integer.MAX_VALUE),
                ex -> ex instanceof IllegalStateException);

        // In recovery mode: setContainerEpoch should work.
        m.enterRecoveryMode();
        m.setContainerEpoch(epoch);
        Assert.assertEquals("Unexpected value from getContainerEpoch.", epoch, m.getContainerEpoch());
        AssertExtensions.assertThrows(
                "setContainerEpoch allowed updating the epoch after the initial set.",
                () -> m.setContainerEpoch(11),
                ex -> ex instanceof IllegalStateException);

        Assert.assertEquals("Unexpected value from getContainerEpoch after rejected update.", epoch, m.getContainerEpoch());

        m.exitRecoveryMode();
        Assert.assertEquals("Unexpected value from getContainerEpoch after exit from recovery mode.", epoch, m.getContainerEpoch());
    }

    @Test
    public void testWrongEpochAfterRestore() {
        final long epoch = -1;
        final StreamSegmentContainerMetadata m = new StreamSegmentContainerMetadata(CONTAINER_ID, 10);
        Assert.assertThrows( IllegalArgumentException.class, () -> m.setContainerEpochAfterRestore(epoch));
    }

    @Test
    public void testWrongSequenceNumberAfterRestore() {
        final long sn = -1;
        final StreamSegmentContainerMetadata m = new StreamSegmentContainerMetadata(CONTAINER_ID, 10);
        Assert.assertThrows( IllegalArgumentException.class, () -> m.setOperationSequenceNumberAfterRestore(sn));
    }

    /**
     * Tests the ability to map new StreamSegments.
     */
    @Test
    public void testMapStreamSegment() {
        final UpdateableContainerMetadata m = new MetadataBuilder(CONTAINER_ID).build();
        final HashMap<Long, Long> segmentIds = new HashMap<>();
        for (long i = 0; i < SEGMENT_COUNT; i++) {
            final long segmentId = segmentIds.size();
            String segmentName = getName(segmentId);

            // This should work.
            m.nextOperationSequenceNumber(); // Change the sequence number, before mapping.
            m.mapStreamSegmentId(segmentName, segmentId);
            segmentIds.put(segmentId, m.getOperationSequenceNumber());
            Assert.assertEquals("Unexpected value from getStreamSegmentId (Stand-alone Segment).", segmentId, m.getStreamSegmentId(segmentName, false));

            // Now check that we cannot re-map the same SegmentId or SegmentName.
            AssertExtensions.assertThrows(
                    "mapStreamSegmentId allowed mapping the same SegmentId twice.",
                    () -> m.mapStreamSegmentId(segmentName + "foo", segmentId),
                    ex -> ex instanceof IllegalArgumentException);
            AssertExtensions.assertThrows(
                    "mapStreamSegmentId allowed mapping the same SegmentName twice.",
                    () -> m.mapStreamSegmentId(segmentName, segmentId + 1),
                    ex -> ex instanceof IllegalArgumentException);
        }

        // Check getLastUsed.
        for (Map.Entry<Long, Long> e : segmentIds.entrySet()) {
            m.nextOperationSequenceNumber(); // Increment the SeqNo so we can verify 'updateLastUsed'.
            SegmentMetadata segmentMetadata = m.getStreamSegmentMetadata(e.getKey());
            Assert.assertEquals("Unexpected value for getLastUsed for untouched segment.", (long) e.getValue(), segmentMetadata.getLastUsed());
            m.getStreamSegmentId(segmentMetadata.getName(), false);
            Assert.assertEquals("Unexpected value for getLastUsed for untouched segment.", (long) e.getValue(), segmentMetadata.getLastUsed());
            m.getStreamSegmentId(segmentMetadata.getName(), true);
            Assert.assertEquals("Unexpected value for getLastUsed for touched segment.", m.getOperationSequenceNumber(), segmentMetadata.getLastUsed());
        }

        Collection<Long> metadataSegmentIds = m.getAllStreamSegmentIds();
        AssertExtensions.assertContainsSameElements("Metadata does not contain the expected Segment Ids", segmentIds.keySet(), metadataSegmentIds);
    }

    /**
     * Tests the ability of the metadata to enforce the Maximum Active Segment Count rule.
     */
    @Test
    public void testMaxActiveSegmentCount() {
        final int maxCount = 2;
        final UpdateableContainerMetadata m = new MetadataBuilder(CONTAINER_ID)
                .withMaxActiveSegmentCount(maxCount)
                .build();

        // Map 2 Segments. These should fill up the capacity.
        m.mapStreamSegmentId("1", 1);
        m.mapStreamSegmentId("2", 2);

        // Verify we cannot map anything now.
        AssertExtensions.assertThrows(
                "Metadata allowed mapping more segments than indicated (segment).",
                () -> m.mapStreamSegmentId("3", 3),
                ex -> ex instanceof IllegalStateException);

        // Verify we are allowed to do this in recovery mode.
        m.enterRecoveryMode();
        m.mapStreamSegmentId("3", 3);
        m.mapStreamSegmentId("4", 4);
        m.exitRecoveryMode();
        Assert.assertNotNull("Metadata did not map new segment that exceeded the quota in recovery mode.", m.getStreamSegmentMetadata(3));
    }

    /**
     * Tests the ability for the metadata to reset itself.
     */
    @Test
    public void testReset() {
        // Segments, Sequence Number + Truncation markers
        final UpdateableContainerMetadata m = new MetadataBuilder(CONTAINER_ID).build();

        // Set a high Sequence Number
        m.enterRecoveryMode();
        m.setOperationSequenceNumber(Integer.MAX_VALUE);
        m.setContainerEpoch(Integer.MAX_VALUE + 1L);
        m.exitRecoveryMode();

        // Populate some StreamSegments.
        ArrayList<Long> segmentIds = new ArrayList<>();
        for (long i = 0; i < SEGMENT_COUNT; i++) {
            final long segmentId = segmentIds.size();
            segmentIds.add(segmentId);
            m.mapStreamSegmentId(getName(segmentId), segmentId);
        }

        // Add some truncation markers.
        final long truncationMarkerSeqNo = 10;
        m.recordTruncationMarker(truncationMarkerSeqNo, new TestLogAddress(truncationMarkerSeqNo));
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
        AssertExtensions.assertLessThan("Epoch was not reset.", 0, m.getContainerEpoch());
        for (long segmentId : segmentIds) {
            Assert.assertEquals("SegmentMetadata was not reset (getStreamSegmentId).", ContainerMetadata.NO_STREAM_SEGMENT_ID, m.getStreamSegmentId(getName(segmentId), false));
            Assert.assertNull("SegmentMetadata was not reset (getStreamSegmentMetadata).", m.getStreamSegmentMetadata(segmentId));
        }

        LogAddress tmSeqNo = m.getClosestTruncationMarker(truncationMarkerSeqNo);
        Assert.assertNull("Truncation Markers were not reset.", tmSeqNo);
        Assert.assertFalse("Truncation Points were not reset.", m.isValidTruncationPoint(truncationMarkerSeqNo));
    }

    /**
     * Tests the Truncation Marker functionality (truncation points is tested separately).
     */
    @Test
    public void testTruncationMarkers() {
        final long maxSeqNo = 1000;
        final int markerFrequency = 13;
        Function<Long, LogAddress> getFrameAddress = seqNo -> new TestLogAddress(Integer.MAX_VALUE + seqNo * seqNo);
        final UpdateableContainerMetadata m = new MetadataBuilder(CONTAINER_ID).build();

        // Record some truncation markers, starting a few steps after initial.
        for (long seqNo = markerFrequency; seqNo <= maxSeqNo; seqNo += markerFrequency) {
            m.recordTruncationMarker(seqNo, getFrameAddress.apply(seqNo));
        }

        // Verify them.
        for (long seqNo = 0; seqNo < maxSeqNo + markerFrequency; seqNo++) {
            LogAddress expectedTruncationMarker = null;
            if (seqNo >= markerFrequency) {
                long input = seqNo > maxSeqNo ? maxSeqNo : seqNo;
                expectedTruncationMarker = getFrameAddress.apply(input - input % markerFrequency);
            }

            LogAddress truncationMarker = m.getClosestTruncationMarker(seqNo);
            Assert.assertEquals("Unexpected truncation marker value for Op Sequence Number " + seqNo, expectedTruncationMarker, truncationMarker);
        }

        // Remove some truncation markers & verify again.
        for (long seqNo = 0; seqNo < maxSeqNo + markerFrequency; seqNo++) {
            m.removeTruncationMarkers(seqNo);

            // Check that the removal actually made sense (it should return -1 now).
            LogAddress truncationMarker = m.getClosestTruncationMarker(seqNo);
            Assert.assertNull("Unexpected truncation marker value after removal for Op Sequence Number " + seqNo, truncationMarker);

            // Check that the next higher up still works.
            long input = seqNo + markerFrequency;
            input = input > maxSeqNo ? maxSeqNo : input;
            LogAddress expectedTruncationMarker;
            if (seqNo > maxSeqNo - markerFrequency) {
                // We have already removed all possible truncation markers, so expect the result to be -1.
                expectedTruncationMarker = null;
            } else {
                expectedTruncationMarker = getFrameAddress.apply(input - input % markerFrequency);
            }

            truncationMarker = m.getClosestTruncationMarker(input);
            Assert.assertEquals(
                "Unexpected truncation marker value for Op Sequence Number " + input + " after removing marker at Sequence Number " + seqNo,
                expectedTruncationMarker, truncationMarker);
        }
    }

    /**
     * Tests the ability to set and retrieve truncation points (truncation markers is tested separately).
     */
    @Test
    public void testValidTruncationPoints() {
        final UpdateableContainerMetadata m = new MetadataBuilder(CONTAINER_ID).build();
        for (int i = 0; i < 100; i += 2) {
            m.setValidTruncationPoint(i);
        }

        for (int i = 0; i < 100; i++) {
            boolean expectedValid = i % 2 == 0;
            Assert.assertEquals("Unexpected result from isValidTruncationPoint.", expectedValid, m.isValidTruncationPoint(i));
        }
    }

    /**
     * Tests the ability to identify Segment Metadatas that are not in use anymore and are eligible for eviction.
     * 1. Creates a number of segments.
     * 2. Truncates repeatedly and at each step verifies that the correct segments were identified as candidates.
     * 3. "Expires" all segments and verifies they are all identified as candidates.
     */
    @Test
    public void testGetEvictionCandidates() {
        // Expire each segment at a different stage.
        final long firstStageExpiration = SEGMENT_COUNT;
        final long finalExpiration = firstStageExpiration + SEGMENT_COUNT;

        // Create a number of segments.
        // Each segment has a 'LastKnownSequenceNumber' set in incremental order.
        final ArrayList<Long> segments = new ArrayList<>();
        final StreamSegmentContainerMetadata m = new MetadataBuilder(CONTAINER_ID).buildAs();
        populateSegmentsForEviction(segments, m);

        for (int i = 0; i < segments.size(); i++) {
            UpdateableSegmentMetadata segmentMetadata = m.getStreamSegmentMetadata(segments.get(i));
            if (i % 2 == 0) {
                // 1/2 of segments expire at the end.
                segmentMetadata.setLastUsed(finalExpiration);
            } else {
                // The rest of the segments expire in the first stage.
                segmentMetadata.setLastUsed(firstStageExpiration);
            }
        }

        // Add one segment that will be deleted. This should be evicted as soon as its LastUsed is before the truncation point.
        final long deletedSegmentId = segments.size();
        UpdateableSegmentMetadata deletedSegment = m.mapStreamSegmentId(getName(deletedSegmentId), deletedSegmentId);
        deletedSegment.markDeleted();
        deletedSegment.setLastUsed(firstStageExpiration);
        segments.add(deletedSegmentId);

        // Verify that not-yet-truncated operations will not be selected for truncation.
        val truncationPoints = Arrays.asList(0L, firstStageExpiration, finalExpiration, finalExpiration + 1);
        Collection<SegmentMetadata> evictionCandidates;
        for (long truncatedSeqNo : truncationPoints) {
            // Simulate a truncation.
            m.removeTruncationMarkers(truncatedSeqNo);

            // Try to evict everything.
            evictionCandidates = m.getEvictionCandidates(finalExpiration + 1, Integer.MAX_VALUE);
            checkEvictedSegmentCandidates(evictionCandidates, m, finalExpiration + 1, truncatedSeqNo);
        }

        // Now we expire all segments.
        evictionCandidates = m.getEvictionCandidates(finalExpiration + 1, Integer.MAX_VALUE);
        checkEvictedSegmentCandidates(evictionCandidates, m, finalExpiration + 1, Long.MAX_VALUE);

        // Check that, in the end, all segments in the metadata have been selected for eviction.
        Assert.assertEquals("Not all segments were evicted.", segments.size(), evictionCandidates.size());
    }

    /**
     * Tests the ability to identify Segment Metadatas that are not in use anymore and are eligible for eviction when
     * there is an upper limit on how many such segments can be evicted at once.
     */
    @Test
    public void testGetEvictionCandidatesCapped() {
        final int maxEvictionCount = SEGMENT_COUNT / 10;
        final ArrayList<Long> segments = new ArrayList<>();
        final StreamSegmentContainerMetadata m = new MetadataBuilder(CONTAINER_ID).buildAs();
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            long segmentId = SEGMENT_COUNT - segments.size();
            m.mapStreamSegmentId(getName(segmentId), segmentId);
            segments.add(segmentId);
        }

        for (int i = 0; i < segments.size(); i++) {
            UpdateableSegmentMetadata segmentMetadata = m.getStreamSegmentMetadata(segments.get(i));
            segmentMetadata.setLastUsed(i);
            m.removeTruncationMarkers(i + 1);
        }

        // Verify that not-yet-truncated operations will not be selected for truncation.
        Collection<SegmentMetadata> evictionCandidates;

        // Expire all segments, one by one, and verify that only at most maxEvictionCount are returned, and when they are
        // capped, only the oldest-used segments are returned, in order.
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            int requestedCount = i + 1;
            evictionCandidates = m.getEvictionCandidates(requestedCount, maxEvictionCount);
            int expectedCount = Math.min(maxEvictionCount, requestedCount);
            Assert.assertEquals("Unexpected number of segments eligible for eviction.", expectedCount, evictionCandidates.size());
            if (requestedCount <= maxEvictionCount) {
                int expectedSegmentIndex = expectedCount - 1;
                for (SegmentMetadata candidate : evictionCandidates) {
                    Assert.assertEquals("Unexpected segment id chosen for eviction when less than Max.",
                            (long) segments.get(expectedSegmentIndex), candidate.getId());
                    expectedSegmentIndex--;
                }
            } else {
                // We were capped - make sure only the oldest-used segments are returned, in order.
                int expectedSegmentIndex = 0;
                for (SegmentMetadata candidate : evictionCandidates) {
                    Assert.assertEquals("Unexpected segment id chosen for eviction when more than Max.",
                            (long) segments.get(expectedSegmentIndex), candidate.getId());
                    expectedSegmentIndex++;
                }
            }
        }
    }

    /**
     * Tests the ability to evict Segment Metadatas that are not in use anymore.
     * 1. Creates a number of segments.
     * 2. Increases the truncated SeqNo in the metadata gradually and at each step verifies that the correct segments were evicted.
     * 3. Expires all segments and verifies they are all evicted.
     */
    @Test
    public void testCleanup() {
        // Expire each Segment at a different stage.
        final StreamSegmentContainerMetadata m = new MetadataBuilder(CONTAINER_ID).buildAs();

        // Create a number of segments.
        // Each segment has a 'LastUsed' set in incremental order.
        final ArrayList<Long> segments = new ArrayList<>();
        populateSegmentsForEviction(segments, m);

        long maxLastUsed = 1;
        for (Long segmentId : segments) {
            UpdateableSegmentMetadata segmentMetadata = m.getStreamSegmentMetadata(segmentId);
            segmentMetadata.setLastUsed(maxLastUsed++);
        }

        final Map<Long, UpdateableSegmentMetadata> segmentMetadatas = segments
                .stream().collect(Collectors.toMap(id -> id, m::getStreamSegmentMetadata));

        // Truncate everything and expire all segments.
        m.removeTruncationMarkers(maxLastUsed);
        Collection<SegmentMetadata> evictionCandidates = m.getEvictionCandidates(maxLastUsed, Integer.MAX_VALUE);

        // Pick 3 segments and touch them. Then verify all but the 3 involved Segments are evicted.
        final long touchedSeqNo = maxLastUsed + 10;
        final ArrayList<Long> touchedSegments = new ArrayList<>();
        val iterator = segments.iterator();
        touchedSegments.add(iterator.next());
        touchedSegments.add(iterator.next());
        touchedSegments.add(iterator.next());
        segmentMetadatas.get(touchedSegments.get(0)).setLastUsed(touchedSeqNo);
        segmentMetadatas.get(touchedSegments.get(1)).setLastUsed(touchedSeqNo);
        segmentMetadatas.get(touchedSegments.get(2)).setLastUsed(touchedSeqNo);

        // Attempt to cleanup the eviction candidates, and even throw in a new truncation (to verify that alone won't trigger the cleanup).
        m.removeTruncationMarkers(touchedSeqNo + 1);
        Collection<SegmentMetadata> evictedSegments = m.cleanup(evictionCandidates, maxLastUsed);
        for (SegmentMetadata sm : evictedSegments) {
            Assert.assertFalse("Evicted segment was not marked as inactive.", sm.isActive());
        }

        // Check that we evicted all eligible segments, and kept the 'touched' ones still.
        Assert.assertEquals("Unexpected number of segments were evicted (first-cleanup).",
                segments.size() - touchedSegments.size(), evictedSegments.size());
        for (long segmentId : touchedSegments) {
            SegmentMetadata sm = m.getStreamSegmentMetadata(segmentId);
            Assert.assertNotNull("Candidate segment that was touched was still evicted (lookup by id)", sm);
            Assert.assertEquals("Candidate segment that was touched was still evicted (lookup by name).",
                    segmentId,
                    m.getStreamSegmentId(sm.getName(), false));
            Assert.assertTrue("Non-evicted segment was marked as inactive.", sm.isActive());
        }

        // Now expire the remaining segments and verify.
        evictionCandidates = m.getEvictionCandidates(touchedSeqNo + 1, Integer.MAX_VALUE);
        evictedSegments = m.cleanup(evictionCandidates, touchedSeqNo + 1);
        for (SegmentMetadata sm : evictedSegments) {
            Assert.assertFalse("Evicted segment was not marked as inactive.", sm.isActive());
        }

        Assert.assertEquals("Unexpected number of segments were evicted (second-cleanup).",
                touchedSegments.size(), evictedSegments.size());
        for (long segmentId : segments) {
            Assert.assertNull("Candidate segment was not evicted (lookup by id)", m.getStreamSegmentMetadata(segmentId));
        }
    }

    /**
     * Tests the ability to evict Segment Metadata instances that are not in use anymore, subject to a cap.
     */
    @Test
    public void testCleanupCapped() {
        final int maxCap = 10;
        final int segmentCount = maxCap * 2;

        // Expire each Segment at a different stage.
        final StreamSegmentContainerMetadata m = new MetadataBuilder(CONTAINER_ID).buildAs();

        // Create a single Segment.
        long maxLastUsed = 1;
        val segments = new ArrayList<Long>();
        for (int i = 0; i < segmentCount; i++) {
            final long segmentId = segments.size();
            segments.add(segmentId);
            m.mapStreamSegmentId("S_" + segmentId, segmentId)
             .setLastUsed(maxLastUsed++);
        }

        // Collect a number of eviction candidates using a stringent max cap (less than the number of segments),
        // then evict those segments.
        m.removeTruncationMarkers(maxLastUsed);
        val evictionCandidates = m.getEvictionCandidates(maxLastUsed, maxCap);
        val evictedSegments = m.cleanup(evictionCandidates, maxLastUsed);
        AssertExtensions.assertGreaterThan("At least one segment was expected to be evicted.", 0, evictedSegments.size());
    }

    /**
     * Tests the ability to pin Segments to the metadata, which should prevent them from being evicted.
     */
    @Test
    public void testPinnedSegments() {
        final StreamSegmentContainerMetadata m = new MetadataBuilder(CONTAINER_ID).buildAs();
        final String segmentName = "segment";
        final String pinnedSegmentName = "segment_pinned";
        final long segmentId = 1;
        final long pinnedSegmentId = 2;

        // Map the segments and pin only one of them.
        val nonPinnedMetadata = m.mapStreamSegmentId(segmentName, segmentId);
        val pinnedMetadata = m.mapStreamSegmentId(pinnedSegmentName, pinnedSegmentId);
        pinnedMetadata.markPinned();

        // Try to evict it.
        nonPinnedMetadata.setLastUsed(1);
        pinnedMetadata.setLastUsed(1);
        m.removeTruncationMarkers(2);
        val evictionCandidates = m.getEvictionCandidates(2, 10);
        Assert.assertEquals("Not expecting pinned segment to be evicted.", 1, evictionCandidates.size());
        val evicted = evictionCandidates.stream().findFirst().get();
        Assert.assertEquals("Unexpected Segment got evicted.", nonPinnedMetadata, evicted);
    }

    private void populateSegmentsForEviction(List<Long> segments, UpdateableContainerMetadata m) {
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            long segmentId = segments.size();
            m.mapStreamSegmentId(getName(segmentId), segmentId);
            segments.add(segmentId);
        }
    }

    private void checkEvictedSegmentCandidates(Collection<SegmentMetadata> candidates, UpdateableContainerMetadata metadata,
                                               long expirationSeqNo, long truncatedSeqNo) {
        long cutoffSeqNo = Math.min(expirationSeqNo, truncatedSeqNo);
        HashSet<Long> candidateIds = new HashSet<>();
        for (SegmentMetadata candidate : candidates) {
            // Check that all segments in candidates are actually eligible for removal.
            boolean isEligible = shouldExpectRemoval(candidate.getId(), metadata, cutoffSeqNo, truncatedSeqNo);
            Assert.assertTrue("Unexpected eviction candidate in segment " + candidate.getId(), isEligible);

            // Check that all segments in candidates are not actually removed from the metadata.
            Assert.assertNotNull("ContainerMetadata no longer has metadata for eviction candidate segment " + candidate.getId(),
                    metadata.getStreamSegmentMetadata(candidate.getId()));
            Assert.assertNotEquals("ContainerMetadata no longer has name mapping for eviction candidate segment " + candidate.getId(),
                    ContainerMetadata.NO_STREAM_SEGMENT_ID, metadata.getStreamSegmentId(candidate.getName(), false));
            candidateIds.add(candidate.getId());
        }

        // Check that all segments remaining in the metadata are still eligible to remain there.
        for (long segmentId : metadata.getAllStreamSegmentIds()) {
            if (!candidateIds.contains(segmentId)) {
                boolean expectedRemoved = shouldExpectRemoval(segmentId, metadata, cutoffSeqNo, truncatedSeqNo);
                Assert.assertFalse("Unexpected non-eviction for segment " + segmentId, expectedRemoved);
            }
        }
    }

    private boolean shouldExpectRemoval(long segmentId, ContainerMetadata m, long cutoffSeqNo, long truncatedSeqNo) {
        SegmentMetadata segmentMetadata = m.getStreamSegmentMetadata(segmentId);
        if (segmentMetadata.isDeleted()) {
            // Deleted segments are immediately eligible for eviction as soon as their last op is truncated out.
            return segmentMetadata.getLastUsed() <= truncatedSeqNo;
        } else {
            return segmentMetadata.getLastUsed() < cutoffSeqNo;
        }
    }

    private String getName(long id) {
        return "Segment" + id;
    }

    private static class TestLogAddress extends LogAddress {
        TestLogAddress(long sequence) {
            super(sequence);
        }

        @Override
        public int hashCode() {
            return Long.hashCode(getSequence());
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof TestLogAddress) {
                return this.getSequence() == ((TestLogAddress) other).getSequence();
            }

            return false;
        }
    }
}
