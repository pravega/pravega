/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Optional;
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
    private final NavigableMap<Double, Segment> segments;
    @Getter
    private final String delegationToken;

    /**
     * Creates a new instance of the StreamSegments class.
     *
     * @param segments Segments of a stream, keyed by the largest key in their key range.
     *                 i.e. If there are two segments split evenly, the first should have a value of 0.5 and the second 1.0.
     * @param delegationToken Delegation token to access the segments in the segmentstore
     */
    public StreamSegments(NavigableMap<Double, Segment> segments, String delegationToken) {
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
        return segments.ceilingEntry(key).getValue();
    }

    public Collection<Segment> getSegments() {
        return segments.values();
    }
    
    public StreamSegments withReplacementRange(Segment replacedSegment, StreamSegmentsWithPredecessors replacementRanges) {
        Preconditions.checkState(segments.containsValue(replacedSegment), "Segment to be replaced should be present in the segment list");
        verifyReplacementRange(replacementRanges);
        NavigableMap<Double, Segment> result = new TreeMap<>();
        Map<Long, List<SegmentWithRange>> replacedRanges = replacementRanges.getReplacementRanges();
        Optional<List<SegmentWithRange>> replacements = Optional.ofNullable(replacedRanges.get(replacedSegment.getSegmentId()));
        Segment lastSegmentValue = null;
        for (Entry<Double, Segment> exitingEntry : segments.descendingMap().entrySet()) { //iterate from the highest key.
            final Segment exitingSegment = exitingEntry.getValue();
            if (exitingSegment.equals(lastSegmentValue)) {
                //last value was the same same segment, it can be consolidated.
                continue;
            }
            if (exitingSegment.equals(replacedSegment)) {
                //segment needs to be replaced.
                replacements.ifPresent(segmentWithRanges -> segmentWithRanges.forEach(segmentWithRange ->
                        result.put(Math.min(segmentWithRange.getHigh(), exitingEntry.getKey()), segmentWithRange.getSegment())));
            } else {
                //update remaining values.
                result.put(exitingEntry.getKey(), exitingEntry.getValue());
            }
            lastSegmentValue = exitingSegment; // update lastSegmentValue to reduce number of entries in the map.
        }
        return new StreamSegments(result, delegationToken);
    }
    
    /**
     * Checks that replacementSegments provided are consistent with the segments that currently being used.
     * @param replacementSegments The StreamSegmentsWithPredecessors to verify
     */
    private void verifyReplacementRange(StreamSegmentsWithPredecessors replacementSegments) {
        log.debug("Verification of replacement segments {} with the current segments {}", replacementSegments, segments);
        Map<Long, List<SegmentWithRange>> replacementRanges = replacementSegments.getReplacementRanges();
        for (Entry<Long, List<SegmentWithRange>> ranges : replacementRanges.entrySet()) {
            double lowerReplacementRange = 1;
            double upperReplacementRange = 0;
            for (SegmentWithRange range : ranges.getValue()) {
                upperReplacementRange = Math.max(upperReplacementRange, range.getHigh());
                lowerReplacementRange = Math.min(lowerReplacementRange, range.getLow());
            }
            Entry<Double, Segment> upperReplacedSegment = segments.floorEntry(upperReplacementRange);
            Entry<Double, Segment> lowerReplacedSegment =  segments.higherEntry(lowerReplacementRange);
            Preconditions.checkArgument(upperReplacedSegment != null, "Missing replaced replacement segments %s",
                                        replacementSegments);
            Preconditions.checkArgument(lowerReplacedSegment != null, "Missing replaced replacement segments %s",
                                        replacementSegments);
         }
    }

    @Override
    public String toString() {
        return "StreamSegments:" + segments.toString();
    }
}
