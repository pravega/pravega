/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.stream.Segment;
import io.pravega.client.stream.StreamSegmentsWithPredecessors;
import io.pravega.common.hash.HashHelper;
import io.pravega.client.stream.SegmentWithRange;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import lombok.EqualsAndHashCode;

/**
 * The segments that within a stream at a particular point in time.
 */
@EqualsAndHashCode
public class StreamSegments {
    private static final HashHelper HASHER = HashHelper.seededWith("EventRouter");
    private final NavigableMap<Double, Segment> segments;

    /**
     * Creates a new instance of the StreamSegments class.
     *
     * @param segments Segments of a stream, keyed by the largest key in their key range.
     *                 i.e. If there are two segments split evenly, the first should have a value of 0.5 and the second 1.0.
     */
    public StreamSegments(NavigableMap<Double, Segment> segments) {
        this.segments = Collections.unmodifiableNavigableMap(segments);
        verifySegments();
    }

    private void verifySegments() {
        if (!segments.isEmpty()) {
            Preconditions.checkArgument(segments.firstKey() > 0.0, "Nonsense value for segment.");
            Preconditions.checkArgument(segments.lastKey() >= 1.0, "Last segment missing.");
            Preconditions.checkArgument(segments.lastKey() < 1.00001, "Segments should only go up to 1.0");
        }
    }
    
    public Segment getSegmentForKey(String key) {
        return getSegmentForKey(HASHER.hashToRange(key));
    }

    public Segment getSegmentForKey(double key) {
        Preconditions.checkArgument(key >= 0.0);
        Preconditions.checkArgument(key <= 1.0);
        return segments.ceilingEntry(key).getValue();
    }

    public Collection<Segment> getSegments() {
        return segments.values();
    }
    
    public StreamSegments withReplacementRange(StreamSegmentsWithPredecessors replacementRanges) {
        NavigableMap<Double, Segment> result = new TreeMap<>();
        Map<Integer, List<SegmentWithRange>> replacedRanges = replacementRanges.getReplacementRanges();
        for (Entry<Double, Segment> exitingSegment : segments.entrySet()) {
            List<SegmentWithRange> replacements = replacedRanges.get(exitingSegment.getValue().getSegmentNumber());
            if (replacements == null || replacements.isEmpty()) {
                result.put(exitingSegment.getKey(), exitingSegment.getValue());
            } else {
                for (SegmentWithRange replacement : replacements) {
                    result.put(replacement.getHigh(), replacement.getSegment());
                }
            }
        }
        return new StreamSegments(result);
    }
}
