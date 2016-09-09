package com.emc.pravega.stream;

import com.emc.pravega.stream.impl.PositionImpl;

import java.util.Map;
import java.util.Set;

public interface PositionInternal {
    /**
     * Set of segments currently being read
     * @return
     */
    Set<SegmentId> getOwnedSegments();

    /**
     * Returns read offset for each segment currently being read.
     * Completely read segments have offset of -1.
     * @return
     */
    Map<SegmentId, Long> getOwnedSegmentsWithOffsets();

    /**
     * Returns the set of completely read segments.
     * @return
     */
    Set<SegmentId> getCompletedSegments();

    /**
     * Read offset for a specific segment contained in the position.
     * @param SegmentId input segment
     * @return
     */
    Long getOffsetForOwnedSegment(SegmentId SegmentId);

    /**
     * futureOwnedSegments are those that can be read after one of the currently read segment is completely read.
     * Each segment in this set has exactly one previous segment that belongs to the set ownedSegments.
     *
     * @return
     */
    Set<SegmentId> getFutureOwnedSegments();

    PositionImpl asInternalImpl();
}
