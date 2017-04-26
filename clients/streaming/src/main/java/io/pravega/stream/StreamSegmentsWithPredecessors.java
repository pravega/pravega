/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;

/**
 * The successor segments of a given segment.
 */
@EqualsAndHashCode
public class StreamSegmentsWithPredecessors {
    private final Map<Segment, List<Integer>> segmentWithPredecessors;
    private final Map<Integer, List<SegmentWithRange>> replacementRanges;

    public StreamSegmentsWithPredecessors(final Map<SegmentWithRange, List<Integer>> segments) {
        segmentWithPredecessors = Collections.unmodifiableMap(segments.entrySet().stream().collect(
                Collectors.toMap(entry -> entry.getKey().getSegment(), Map.Entry::getValue)));

        Map<Integer, List<SegmentWithRange>> replacementRanges = new HashMap<>();
        for (Entry<SegmentWithRange, List<Integer>> entry : segments.entrySet()) {
            for (Integer oldSegment : entry.getValue()) {
                List<SegmentWithRange> newRanges = replacementRanges.get(oldSegment);
                if (newRanges == null) {
                    newRanges = new ArrayList<>(2);
                    replacementRanges.put(oldSegment, newRanges);
                }
                newRanges.add(entry.getKey());
            }
        }
        this.replacementRanges = Collections.unmodifiableMap(replacementRanges);
    }

    /**
     * Get Segment to Predecessor mapping.
     *
     * @return Map<Segment, List<Integer>> Segment to Predecessor mapping.
     */
    public Map<Segment, List<Integer>> getSegmentToPredecessor() {
        return segmentWithPredecessors;
    }
    
    /**
     * @return A map of the segment numbers that have been replaced mapped to their replacement segment/ranges
     */
    public Map<Integer, List<SegmentWithRange>> getReplacementRanges() {
        return replacementRanges;
    }

}
