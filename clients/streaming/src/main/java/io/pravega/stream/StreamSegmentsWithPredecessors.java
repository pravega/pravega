/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package io.pravega.stream;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.apache.commons.lang.math.DoubleRange;

/**
 * The successor segments of a given segment.
 */
@EqualsAndHashCode
public class StreamSegmentsWithPredecessors {
    private final Map<Segment, List<Integer>> segmentWithPredecessors;
    private final Map<Segment, DoubleRange> segmentWithKeyRange;

    public StreamSegmentsWithPredecessors(final Map<SegmentWithRange, List<Integer>> segments) {
        segmentWithPredecessors = Collections.unmodifiableMap(segments.entrySet().stream().collect(
                Collectors.toMap(entry -> entry.getKey().getSegment(), Map.Entry::getValue)));

        segmentWithKeyRange = Collections.unmodifiableMap(segments.entrySet().stream().collect(
                Collectors.toMap(entry -> entry.getKey().getSegment(),
                        entry -> new DoubleRange(entry.getKey().getLow(), entry.getKey().getHigh()))));
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
     * @return A set of the segment numbers that have been replaced by these.
     */
    public Set<Integer> getReplacedSegments() {
        return segmentWithPredecessors.values().stream().flatMap(l -> l.stream()).collect(Collectors.toSet());
    }

    /**
     * Returns the range for a segment
     * 
     * @param segment the segment to get the range for.
     * @return the range for the segment or null if it is not in this data structure.
     */
    public DoubleRange getRangeForSegment(Segment segment) {
        return segmentWithKeyRange.get(segment);
    }
    
    /**
     * Get Segment to Key Range mapping.
     *
     * @return Map<Segment, Range> segment to range mapping.
     */
    public Map<Segment, DoubleRange> getSegmentToRange() {
        return segmentWithKeyRange;
    }
}
