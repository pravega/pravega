/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.common.hash.HashHelper;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * The segments that within a stream at a particular point in time.
 */
@EqualsAndHashCode
@Slf4j
public class StreamSegments {
    private static final HashHelper HASHER = HashHelper.seededWith("EventRouter");
    
    /**
     * Maps the upper end of a range to the corresponding segment. The range in the value is the
     * range of keyspace the segment has been assigned. The range in the value is NOT the same as
     * the difference between two keys. The keys correspond to the range that the client
     * should route to, where as the one in the value is the range the segment it assigned. These
     * may be different if a client still has a preceding segment in its map. In which case a
     * segment's keys may not contain the full assigned range.
     */
    private final NavigableMap<Double, SegmentWithRange> segments;
    @Getter
    private final String delegationToken;

    /**
     * Creates a new instance of the StreamSegments class.
     *
     * @param segments Segments of a stream, keyed by the largest key in their key range.
     *                 i.e. If there are two segments split evenly, the first should have a value of 0.5 and the second 1.0.
     * @param delegationToken Delegation token to access the segments in the segmentstore
     */
    public StreamSegments(NavigableMap<Double, SegmentWithRange> segments, String delegationToken) {
        this.segments = Collections.unmodifiableNavigableMap(segments);
        this.delegationToken = delegationToken;
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
        return segments.ceilingEntry(key).getValue().getSegment();
    }

    public Collection<Segment> getSegments() {
        ArrayList<Segment> result = new ArrayList<>(segments.size());
        for (SegmentWithRange seg : segments.values()) {
            result.add(seg.getSegment());
        }
        return result;
    }
    
    public StreamSegments withReplacementRange(Segment segment, StreamSegmentsWithPredecessors replacementRanges) {
        SegmentWithRange replacedSegment = findReplacedSegment(segment);
        verifyReplacementRange(replacedSegment, replacementRanges);
        NavigableMap<Double, SegmentWithRange> result = new TreeMap<>();
        Map<Long, List<SegmentWithRange>> replacedRanges = replacementRanges.getReplacementRanges();
        List<SegmentWithRange> replacements = replacedRanges.get(segment.getSegmentId());
        Preconditions.checkNotNull(replacements, "Empty set of replacements for: {}", segment.getSegmentId());
        replacements.sort(Comparator.comparingDouble(SegmentWithRange::getHigh).reversed());
        verifyContinuous(replacements);
        for (Entry<Double, SegmentWithRange> existingEntry : segments.descendingMap().entrySet()) { // iterate from the highest key.
            final SegmentWithRange existingSegment = existingEntry.getValue();
            if (existingSegment.equals(replacedSegment)) { // Segment needs to be replaced.
                // Invariant: The successor segment(s)'s range should be limited to the replaced segment's range, thereby
                // ensuring that newer writes to the successor(s) happen only for the replaced segment's range.
                for (SegmentWithRange segmentWithRange : replacements) {
                    Double lowerBound = segments.lowerKey(existingEntry.getKey()); // Used to skip over items not in the clients view yet.
                    if (lowerBound == null || segmentWithRange.getHigh() >= lowerBound) { 
                        result.put(Math.min(segmentWithRange.getHigh(), existingEntry.getKey()), segmentWithRange);
                    }
                }
            } else {
                // update remaining values.
                result.put(existingEntry.getKey(), existingEntry.getValue());
            }
        }
        removeDuplicates(result);
        return new StreamSegments(result, delegationToken);
    }
    
    /**
     * This combines consecutive entries in the map that refer to the same segment.
     * This happens following a merge because the preceeding segments are replaced one at a time.
     */
    private void removeDuplicates(NavigableMap<Double, SegmentWithRange> result) {
        Segment last = null;
        for (Iterator<SegmentWithRange> iterator = result.descendingMap().values().iterator(); iterator.hasNext();) {
            SegmentWithRange current = iterator.next();
            if (current.getSegment().equals(last)) {
                iterator.remove();
            }
            last = current.getSegment();
        }
    }

    private SegmentWithRange findReplacedSegment(Segment segment) {
        return segments.values()
                       .stream()
                       .filter(withRange -> withRange.getSegment().equals(segment))
                       .findFirst()
                       .orElseThrow(() -> new IllegalArgumentException("Segment to be replaced should be present in the segment list"));
    }
    
    /**
     * Checks that replacementSegments provided are consistent with the segments that are currently being used.
     * @param replacedSegment The segment on which EOS was reached
     * @param replacementSegments The StreamSegmentsWithPredecessors to verify
     */
    private void verifyReplacementRange(SegmentWithRange replacedSegment, StreamSegmentsWithPredecessors replacementSegments) {
        log.debug("Verification of replacement segments {} with the current segments {}", replacementSegments, segments);
        Map<Long, List<SegmentWithRange>> replacementRanges = replacementSegments.getReplacementRanges();
        List<SegmentWithRange> replacements = replacementRanges.get(replacedSegment.getSegment().getSegmentId());
        Preconditions.checkArgument(replacements != null, "Replacement segments did not contain replacements for segment being replaced");
        if (replacementRanges.size() == 1) {
            //Simple split
            Preconditions.checkArgument(replacedSegment.getHigh() == getUpperBound(replacements));
            Preconditions.checkArgument(replacedSegment.getLow() == getLowerBound(replacements));
        } else {
            Preconditions.checkArgument(replacedSegment.getHigh() <= getUpperBound(replacements));
            Preconditions.checkArgument(replacedSegment.getLow() >= getLowerBound(replacements));
        }
        for (Entry<Long, List<SegmentWithRange>> ranges : replacementRanges.entrySet()) {
            Entry<Double, SegmentWithRange> upperReplacedSegment = segments.floorEntry(getUpperBound(ranges.getValue()));
            Entry<Double, SegmentWithRange> lowerReplacedSegment = segments.higherEntry(getLowerBound(ranges.getValue()));
            Preconditions.checkArgument(upperReplacedSegment != null, "Missing replaced replacement segments %s",
                                        replacementSegments);
            Preconditions.checkArgument(lowerReplacedSegment != null, "Missing replaced replacement segments %s",
                                        replacementSegments);
        }
    }
    
    private void verifyContinuous(List<SegmentWithRange> newSegments) {
        double previous = newSegments.get(0).getHigh();
        for (SegmentWithRange s : newSegments) {
            Preconditions.checkArgument(previous == s.getHigh(), "Replacement segments were not continious: {}", newSegments);
            previous = s.getLow();
        }
    }

    private double getLowerBound(List<SegmentWithRange> values) {
        double lowerReplacementRange = 1;
        for (SegmentWithRange range : values) {
            lowerReplacementRange = Math.min(lowerReplacementRange, range.getLow());
        }
        return lowerReplacementRange;
    }

    private double getUpperBound(List<SegmentWithRange> value) {
        double upperReplacementRange = 0;
        for (SegmentWithRange range : value) {
            upperReplacementRange = Math.max(upperReplacementRange, range.getHigh());
        }
        return upperReplacementRange;
    }

    @Override
    public String toString() {
        return "StreamSegments:" + segments.toString();
    }
}
