/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.batch;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.StreamCut;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import lombok.val;

import static java.util.stream.Collectors.summarizingInt;

/**
 * This class contains the segment information of a stream between two StreamCuts.
 */
@Data
@Builder
public class StreamSegmentInfo {

    @NonNull
    @Setter(AccessLevel.NONE)
    private final StreamCut startStreamCut;
    @NonNull
    @Setter(AccessLevel.NONE)
    private final StreamCut endStreamCut;
    @NonNull
    @Setter(AccessLevel.NONE)
    private final Iterator<SegmentInputSplit> segmentInputSplitIterator;

    public static StreamSegmentInfoBuilder builder() {
        return new StreamSegmentInfoValidator();
    }

    private static class StreamSegmentInfoValidator extends StreamSegmentInfoBuilder {
        @Override
        public StreamSegmentInfo build() {
            validateStreamCuts(super.startStreamCut, super.endStreamCut);
            return super.build();
        }
    }

    public static void validateStreamCuts(final StreamCut startStreamCut, final StreamCut endStreamCut) {
        //Validate that startStreamCut and endStreamCut are for the same stream.
        Preconditions.checkArgument(startStreamCut.getStream().equals(endStreamCut.getStream()));

        //Validate that two streamCuts are not overlapping.
        Map<Segment, Long> startSegments = startStreamCut.getPositions();
        Map<Segment, Long> endSegments = endStreamCut.getPositions();
        val startSCSummary = startSegments.keySet().stream().collect(summarizingInt(Segment::getSegmentNumber));
        val endSCSummary = endSegments.keySet().stream().collect(summarizingInt(Segment::getSegmentNumber));

        //Ensure no overlap.
        Preconditions.checkState(startSCSummary.getMin() <= endSCSummary.getMin());
        Preconditions.checkState(startSCSummary.getMax() <= endSCSummary.getMax());

        //Ensure that the offsets of overlapping segments does not decrease from startStreamCut to endStreamCut.
        val commonSegments = startSegments.keySet().stream().filter(endSegments::containsKey)
                                          .collect(Collectors.toList());
        commonSegments.forEach(s -> Preconditions.checkState(startSegments.get(s) <= endSegments.get(s)));
    }
}
