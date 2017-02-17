/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.service.server.containers;

import com.emc.pravega.service.server.ContainerMetadata;
import com.emc.pravega.service.server.ManualTimer;
import com.emc.pravega.service.server.SegmentMetadata;
import com.emc.pravega.service.server.UpdateableContainerMetadata;
import com.emc.pravega.service.storage.LogAddress;
import com.emc.pravega.testcommon.AssertExtensions;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for StreamSegmentContainerMetadata class.
 */
public class StreamSegmentContainerMetadataTests {
    private static final int CONTAINER_ID = 1234567;
    private static final int SEGMENT_COUNT = 100;
    private static final int TRANSACTIONS_PER_SEGMENT_COUNT = 2;

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
     * Tests the ability to map new StreamSegments (as well as Transactions).
     */
    @Test
    public void testMapStreamSegment() {
        final long startTime = 123;
        final ManualTimer timer = new ManualTimer();
        timer.setElapsedMillis(startTime);
        final StreamSegmentContainerMetadata m = new StreamSegmentContainerMetadata(CONTAINER_ID, timer);
        final ArrayList<Long> segmentIds = new ArrayList<>();
        for (long i = 0; i < SEGMENT_COUNT; i++) {
            final long segmentId = segmentIds.size();
            segmentIds.add(segmentId);
            String segmentName = getName(segmentId);

            // This should work.
            m.mapStreamSegmentId(segmentName, segmentId);
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

            for (long j = 0; j < TRANSACTIONS_PER_SEGMENT_COUNT; j++) {
                final long transactionId = segmentIds.size();
                segmentIds.add(transactionId);
                String transactionName = getName(transactionId);

                AssertExtensions.assertThrows(
                        "mapStreamSegmentId allowed mapping a Transaction to an inexistent parent.",
                        () -> m.mapStreamSegmentId(transactionName, transactionId, transactionId),
                        ex -> ex instanceof IllegalArgumentException);

                // This should work.
                m.mapStreamSegmentId(transactionName, transactionId, segmentId);
                Assert.assertEquals("Unexpected value from getStreamSegmentId (Transaction Segment).", transactionId, m.getStreamSegmentId(transactionName, false));

                // Now check that we cannot re-map the same Transaction Id or Name.
                AssertExtensions.assertThrows(
                        "mapStreamSegmentId allowed mapping the same Transaction SegmentId twice.",
                        () -> m.mapStreamSegmentId(transactionName + "foo", transactionId, segmentId),
                        ex -> ex instanceof IllegalArgumentException);
                AssertExtensions.assertThrows(
                        "mapStreamSegmentId allowed mapping the same Transaction SegmentName twice.",
                        () -> m.mapStreamSegmentId(transactionName, transactionId + 1, segmentId),
                        ex -> ex instanceof IllegalArgumentException);

                // Now check that we cannot map a Transaction to another Transaction.
                AssertExtensions.assertThrows(
                        "mapStreamSegmentId allowed mapping the a Transaction to another Transaction.",
                        () -> m.mapStreamSegmentId(transactionName + "foo", transactionId + 1, transactionId),
                        ex -> ex instanceof IllegalArgumentException);
            }
        }

        // Check lastKnownRequestTime.
        final long newTime = startTime + 1000;
        timer.setElapsedMillis(newTime);
        for (long segmentId : segmentIds) {
            StreamSegmentMetadata segmentMetadata = (StreamSegmentMetadata) m.getStreamSegmentMetadata(segmentId);
            Assert.assertEquals("Unexpected value for getLastKnownRequestTime for untouched segment.", startTime, segmentMetadata.getLastKnownRequestTime());
            m.getStreamSegmentId(segmentMetadata.getName(), false);
            Assert.assertEquals("Unexpected value for getLastKnownRequestTime for untouched segment.", startTime, segmentMetadata.getLastKnownRequestTime());
            m.getStreamSegmentId(segmentMetadata.getName(), true);
            Assert.assertEquals("Unexpected value for getLastKnownRequestTime for touched segment.", newTime, segmentMetadata.getLastKnownRequestTime());
        }

        Collection<Long> metadataSegmentIds = m.getAllStreamSegmentIds();
        AssertExtensions.assertContainsSameElements("Metadata does not contain the expected Segment Ids", segmentIds, metadataSegmentIds);
    }

    /**
     * Tests the ability to delete a StreamSegment from the metadata, as well as any dependent (Transaction) StreamSegments.
     */
    @Test
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void testDeleteStreamSegment() {
        StreamSegmentContainerMetadata m = new StreamSegmentContainerMetadata(CONTAINER_ID);
        ArrayList<Long> segmentIds = new ArrayList<>();
        for (long i = 0; i < SEGMENT_COUNT; i++) {
            final long segmentId = segmentIds.size();
            segmentIds.add(segmentId);
            m.mapStreamSegmentId(getName(segmentId), segmentId);
            for (long j = 0; j < TRANSACTIONS_PER_SEGMENT_COUNT; j++) {
                final long transactionId = segmentIds.size();
                segmentIds.add(transactionId);
                m.mapStreamSegmentId(getName(transactionId), transactionId, segmentId);
            }
        }

        // By construction (see above, any index i=3n is a parent StreamSegment, and any index i=3n+1 or 3n+2 is a Transaction).
        // Let's delete a few parent StreamSegments and verify their Transactions are also deleted.
        // Then delete only Transactions, and verify those are the only ones to be deleted.
        final int groupSize = TRANSACTIONS_PER_SEGMENT_COUNT + 1;
        ArrayList<Integer> streamSegmentsToDelete = new ArrayList<>();
        ArrayList<Integer> transactionsToDelete = new ArrayList<>();
        for (int i = 0; i < segmentIds.size(); i++) {
            if (i < segmentIds.size() / 2) {
                // In the first half, we only delete the parents (which will force the Transactions to be deleted too).
                if (i % groupSize == 0) {
                    streamSegmentsToDelete.add(i);
                }
            } else {
                // In the second half, we only delete the first Transaction of any segment.
                if (i % groupSize == 1) {
                    transactionsToDelete.add(i);
                }
            }
        }

        // Delete stand-alone StreamSegments (and verify Transactions are also deleted).
        Collection<Long> deletedStreamSegmentIds = new HashSet<>();
        for (int index : streamSegmentsToDelete) {
            long segmentId = segmentIds.get(index);
            String name = m.getStreamSegmentMetadata(segmentId).getName();
            Collection<String> expectedDeletedSegmentNames = new ArrayList<>();
            expectedDeletedSegmentNames.add(name);
            deletedStreamSegmentIds.add(segmentId);
            for (int transIndex = 0; transIndex < TRANSACTIONS_PER_SEGMENT_COUNT; transIndex++) {
                long transactionId = segmentIds.get(index + transIndex + 1);
                deletedStreamSegmentIds.add(transactionId);
                expectedDeletedSegmentNames.add(m.getStreamSegmentMetadata(transactionId).getName());
            }

            Collection<String> deletedSegmentNames = extract(m.deleteStreamSegment(name), SegmentMetadata::getName);
            AssertExtensions.assertContainsSameElements("Unexpected StreamSegments were deleted.", expectedDeletedSegmentNames, deletedSegmentNames);
        }

        // Delete Transactions.
        for (int index : transactionsToDelete) {
            long transactionId = segmentIds.get(index);
            String name = m.getStreamSegmentMetadata(transactionId).getName();
            Collection<String> expectedDeletedSegmentNames = new ArrayList<>();
            deletedStreamSegmentIds.add(transactionId);
            expectedDeletedSegmentNames.add(name);

            Collection<String> deletedSegmentNames = extract(m.deleteStreamSegment(name), SegmentMetadata::getName);
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
            for (long j = 0; j < TRANSACTIONS_PER_SEGMENT_COUNT; j++) {
                final long transactionId = segmentIds.size();
                segmentIds.add(transactionId);
                m.mapStreamSegmentId(getName(transactionId), transactionId, segmentId);
            }
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
        StreamSegmentContainerMetadata m = new StreamSegmentContainerMetadata(CONTAINER_ID);

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

    /**
     * Tests the ability to identify Segment Metadatas that are not in use anymore and are eligible for eviction.
     * 1. Creates a number of segment, and 1/4 of them have transactions.
     * 2. All transactions are set to expire at a particular time and the segments expire in two separate stages.
     * 3. Increases the truncated SeqNo in the metadata gradually and at each step verifies that the correct segments were identified as candidates.
     * 4. Expires all transactions and verifies that all dependent segments (which are eligible) are also identified.
     * 5. Expires all segments and verifies they are all identified as candidates.
     */
    @Test
    public void testGetCleanupCandidates() {
        // Expire each Segment at a different stage.
        final long startTime = 123;
        final long firstStageExpiration = startTime + 1000;
        final long transactionExpiration = firstStageExpiration + 1000;
        final long finalExpiration = transactionExpiration + 1000;

        final ManualTimer timer = new ManualTimer();
        timer.setElapsedMillis(startTime);
        final StreamSegmentContainerMetadata m = new StreamSegmentContainerMetadata(CONTAINER_ID, timer);

        // Create a number of segments, out of which every 4th one has a transaction (25%).
        // Each segment has a 'LastKnownSequenceNumber' set in incremental order.
        final ArrayList<Long> segments = new ArrayList<>();
        final HashMap<Long, Long> transactions = new HashMap<>();
        populateSegmentsForEviction(segments, transactions, m);

        for (int i = 0; i < segments.size(); i++) {
            SegmentMetadata segmentMetadata = m.getStreamSegmentMetadata(segments.get(i));
            if (segmentMetadata.getParentId() != ContainerMetadata.NO_STREAM_SEGMENT_ID) {
                // All transactions expire at once, in a second step.
                timer.setElapsedMillis(transactionExpiration);
            } else if (i % 2 == 0) {
                // 1/2 of segments expire at the end.
                timer.setElapsedMillis(finalExpiration);
            } else {
                // The rest of the segments expire in the first stage.
                timer.setElapsedMillis(firstStageExpiration);
            }

            m.getStreamSegmentId(segmentMetadata.getName(), true);
        }
        // We truncate one by one, and then verify the outcome.
        Collection<SegmentMetadata> evictionCandidates;
        timer.setElapsedMillis(finalExpiration);
        final Duration firstStageTimespan = Duration.ofMillis(finalExpiration - firstStageExpiration - 1);
        for (long segmentId : segments) {
            // "Truncate" the metadata up to this sequence number.
            final long truncatedSeqNo = m.getStreamSegmentMetadata(segmentId).getLastKnownSequenceNumber();
            m.removeTruncationMarkers(truncatedSeqNo);
            evictionCandidates = m.getEvictionCandidates(firstStageTimespan);
            checkEvictedSegmentCandidates(evictionCandidates, transactions, m, firstStageExpiration, truncatedSeqNo);
        }

        // Now we expire transactions.
        final Duration transactionStageTimespan = Duration.ofMillis(timer.getElapsedMillis() - transactionExpiration - 1);
        evictionCandidates = m.getEvictionCandidates(transactionStageTimespan);
        checkEvictedSegmentCandidates(evictionCandidates, transactions, m, transactionExpiration, Long.MAX_VALUE);

        // Now we expire all segments.
        timer.setElapsedMillis(finalExpiration + 10);
        final Duration finalStageTimespan = Duration.ofMillis(timer.getElapsedMillis() - finalExpiration - 1);
        evictionCandidates = m.getEvictionCandidates(finalStageTimespan);
        checkEvictedSegmentCandidates(evictionCandidates, transactions, m, finalExpiration, Long.MAX_VALUE);

        // Check that, in the end, all segments in the metadata have been selected for eviction.
        Assert.assertEquals("Not all segments were evicted.", segments.size(), evictionCandidates.size());
    }

    /**
     * Tests the ability to evict Segment Metadatas that are not in use anymore.
     * 1. Creates a number of segment, and 1/4 of them have transactions.
     * 2. All transactions are set to expire at a particular time and the segments expire in two separate stages.
     * 3. Increases the truncated SeqNo in the metadata gradually and at each step verifies that the correct segments were evicted.
     * 4. Expires all transactions and verifies that all dependent segments (which are eligible) are also evicted.
     * 5. Expires all segments and verifies they are all evicted.
     */
    @Test
    public void testCleanup() {
        // Expire each Segment at a different stage.
        final long startTime = 1000;
        final long getCandidatesTime = startTime + 1000;
        final long cleanupTime = getCandidatesTime + 1000;
        final long finalTime = cleanupTime + 1000;
        final Duration expirationTime = Duration.ofMillis(20);
        final ManualTimer timer = new ManualTimer();
        timer.setElapsedMillis(startTime);
        final StreamSegmentContainerMetadata m = new StreamSegmentContainerMetadata(CONTAINER_ID, timer);

        // Create a number of segments, out of which every 4th one has a transaction (25%).
        // Each segment has a 'LastKnownSequenceNumber' set in incremental order.
        final ArrayList<Long> segments = new ArrayList<>();
        final HashMap<Long, Long> transactions = new HashMap<>();
        populateSegmentsForEviction(segments, transactions, m);

        for (Long segmentId : segments) {
            SegmentMetadata segmentMetadata = m.getStreamSegmentMetadata(segmentId);
            m.getStreamSegmentId(segmentMetadata.getName(), true);
        }

        final Map<Long, SegmentMetadata> segmentMetadatas = segments
                .stream().collect(Collectors.toMap(id -> id, m::getStreamSegmentMetadata));

        // We truncate one by one, and then verify the outcome.
        long maxSeqNo = segmentMetadatas.values().stream().mapToLong(SegmentMetadata::getLastKnownSequenceNumber).max().orElse(-1);
        m.removeTruncationMarkers(maxSeqNo);

        // We expire all segments.
        timer.setElapsedMillis(getCandidatesTime);
        Collection<SegmentMetadata> evictionCandidates = m.getEvictionCandidates(expirationTime);

        // Now pick a Transaction and a non-related Segment and touch them. Then verify all but the three involved
        // Segments are evicted.
        timer.setElapsedMillis(cleanupTime);
        final ArrayList<Long> touchedSegments = new ArrayList<>();
        val iterator = transactions.entrySet().iterator();
        touchedSegments.add(iterator.next().getKey());
        val second = iterator.next();
        touchedSegments.add(second.getValue());
        m.getStreamSegmentId(segmentMetadatas.get(touchedSegments.get(0)).getName(), true);
        m.getStreamSegmentId(segmentMetadatas.get(touchedSegments.get(1)).getName(), true);
        touchedSegments.add(second.getKey()); // We add the Transaction's parent, but do not touch it.

        Collection<SegmentMetadata> evictedSegments = m.cleanup(evictionCandidates, expirationTime);

        // Check that we evicted all eligible segments, and kept the 'touched' ones still.
        Assert.assertEquals("Unexpected number of segments were evicted (first-cleanup).",
                segments.size() - touchedSegments.size(), evictedSegments.size());
        for (long segmentId : touchedSegments) {
            SegmentMetadata sm = m.getStreamSegmentMetadata(segmentId);
            Assert.assertNotNull("Candidate segment that was touched was still evicted (lookup by id)", sm);
            Assert.assertEquals("Candidate segment that was touched was still evicted (lookup by name).",
                    segmentId,
                    m.getStreamSegmentId(sm.getName(), false));
        }

        // Now expire the remaining segments and verify.
        timer.setElapsedMillis(finalTime);
        evictionCandidates = m.getEvictionCandidates(expirationTime);
        evictedSegments = m.cleanup(evictionCandidates, expirationTime);

        Assert.assertEquals("Unexpected number of segments were evicted (second-cleanup).",
                touchedSegments.size(), evictedSegments.size());
        for (long segmentId : segments) {
            Assert.assertNull("Candidate segment was not evicted (lookup by id)", m.getStreamSegmentMetadata(segmentId));
        }
    }

    private void populateSegmentsForEviction(List<Long> segments, Map<Long, Long> transactions, UpdateableContainerMetadata m) {
        for (int i = 0; i < SEGMENT_COUNT; i++) {
            long parentSegmentId = segments.size();
            StreamSegmentMetadata parentMetadata = (StreamSegmentMetadata) m.mapStreamSegmentId(getName(parentSegmentId), parentSegmentId);
            parentMetadata.setLastKnownSequenceNumber(m.nextOperationSequenceNumber());
            segments.add(parentSegmentId);
            if (i % 4 == 0) {
                long transactionId = segments.size();
                StreamSegmentMetadata transactionMetadata = (StreamSegmentMetadata) m.mapStreamSegmentId(getName(parentSegmentId) + "_Transaction", transactionId, parentSegmentId);
                segments.add(transactionId);
                transactionMetadata.setLastKnownSequenceNumber(m.nextOperationSequenceNumber());
                transactions.put(parentSegmentId, transactionId);
            }
        }
    }

    private void checkEvictedSegmentCandidates(Collection<SegmentMetadata> candidates, Map<Long, Long> transactions,
                                               UpdateableContainerMetadata metadata, long expirationTime, long truncatedSeqNo) {
        HashSet<Long> candidateIds = new HashSet<>();
        for (SegmentMetadata candidate : candidates) {
            // Check that all segments in candidates are actually eligible for removal.
            boolean isEligible = shouldExpectRemoval(candidate.getId(), metadata, transactions, expirationTime, truncatedSeqNo);
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
                boolean expectedRemoved = shouldExpectRemoval(segmentId, metadata, transactions, expirationTime, truncatedSeqNo);
                Assert.assertFalse("Unexpected non-eviction for segment " + segmentId, expectedRemoved);
            }
        }
    }

    private boolean shouldExpectRemoval(long segmentId, ContainerMetadata m, Map<Long, Long> transactions,
                                        long expirationTime, long truncatedSeqNo) {
        SegmentMetadata segmentMetadata = m.getStreamSegmentMetadata(segmentId);
        SegmentMetadata transactionMetadata = null;
        if (transactions.containsKey(segmentId)) {
            transactionMetadata = m.getStreamSegmentMetadata(transactions.get(segmentId));
        }

        boolean expectedRemoved = shouldExpectRemoval(segmentMetadata, expirationTime, truncatedSeqNo);
        if (transactionMetadata != null) {
            expectedRemoved &= shouldExpectRemoval(transactionMetadata, expirationTime, truncatedSeqNo);
        }

        return expectedRemoved;
    }

    private boolean shouldExpectRemoval(SegmentMetadata segmentMetadata, long expirationTime, long truncatedSeqNo) {
        return segmentMetadata.getLastKnownSequenceNumber() <= truncatedSeqNo
                && segmentMetadata.getLastKnownRequestTime() <= expirationTime;
    }

    private String getName(long id) {
        return "Segment" + id;
    }

    private <T> Collection<T> extract(Collection<SegmentMetadata> source, Function<SegmentMetadata, T> extractor) {
        return source.stream().map(extractor).collect(Collectors.toList());
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
