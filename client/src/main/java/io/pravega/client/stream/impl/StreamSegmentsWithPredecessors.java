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

import io.pravega.client.segment.impl.Segment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The successor segments of a given segment.
 */
@EqualsAndHashCode
@ToString(exclude = "delegationToken")
public class StreamSegmentsWithPredecessors {
    private final Map<Segment, List<Long>> segmentWithPredecessors;
    private final Map<Long, List<SegmentWithRange>> replacementRanges;
    @Getter
    private final String delegationToken;

    public StreamSegmentsWithPredecessors(final Map<SegmentWithRange, List<Long>> segments, String delegationToken) {
        segmentWithPredecessors = Collections.unmodifiableMap(segments.entrySet().stream().collect(
                Collectors.toMap(entry -> entry.getKey().getSegment(), Map.Entry::getValue)));

        Map<Long, List<SegmentWithRange>> replacementRanges = new HashMap<>();
        for (Entry<SegmentWithRange, List<Long>> entry : segments.entrySet()) {
            for (Long oldSegment : entry.getValue()) {
                List<SegmentWithRange> newRanges = replacementRanges.get(oldSegment);
                if (newRanges == null) {
                    newRanges = new ArrayList<>(2);
                    replacementRanges.put(oldSegment, newRanges);
                }
                newRanges.add(entry.getKey());
            }
        }
        this.replacementRanges = Collections.unmodifiableMap(replacementRanges);
        this.delegationToken = delegationToken;
    }

    /**
     * Get Segment to Predecessor mapping.
     *
     * @return A {@link Map} with {@link Segment} as key and {@link List} of {@link Integer} as value.
     */
    public Map<Segment, List<Long>> getSegmentToPredecessor() {
        return segmentWithPredecessors;
    }

    /**
     * Returns a map of the segment numbers to segment/ranges. The segment numbers (keys) comprise
     * the predecessor segments, while the segment/ranges comprise the successor segments and their
     * corresponding ranges.
     * 
     * @return Predecessors mapped to successors.
     */
    public Map<Long, List<SegmentWithRange>> getReplacementRanges() {
        return replacementRanges;
    }

}
