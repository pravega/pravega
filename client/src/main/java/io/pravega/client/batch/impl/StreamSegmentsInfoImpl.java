/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.batch.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.batch.SegmentInputSplit;
import io.pravega.client.batch.StreamSegmentsInfo;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.StreamCut;
import java.util.Iterator;
import java.util.Map;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * This class contains the segment information of a stream between two StreamCuts.
 */
@Data
@Builder
public class StreamSegmentsInfoImpl implements StreamSegmentsInfo {

    @NonNull
    private final StreamCut startStreamCut;

    @NonNull
    private final StreamCut endStreamCut;

    @NonNull
    private final Iterator<SegmentInputSplit> segmentInputSplitIterator;

    public static StreamSegmentsInfoImplBuilder builder() {
        return new StreamSegmentsImplValidator();
    }

    @Override
    public StreamSegmentsInfoImpl asImpl() {
        return this;
    }

    private static class StreamSegmentsImplValidator extends StreamSegmentsInfoImplBuilder {
        @Override
        public StreamSegmentsInfoImpl build() {
            validateStreamCuts(super.startStreamCut, super.endStreamCut);
            return super.build();
        }
    }

    public static void validateStreamCuts(final StreamCut startStreamCut, final StreamCut endStreamCut) {
        //Validate that startStreamCut and endStreamCut are for the same stream.
        Preconditions.checkArgument(startStreamCut.getStream().equals(endStreamCut.getStream()),
                "startStreamCut and endStreamCut should be for the same stream.");

        final Map<Segment, Long> startSegments = startStreamCut.getPositions();
        final Map<Segment, Long> endSegments = endStreamCut.getPositions();
        //Ensure that the offsets of overlapping segments does not decrease from startStreamCut to endStreamCut.
        startSegments.keySet().stream().filter(endSegments::containsKey)
                     .forEach(s -> Preconditions.checkState(startSegments.get(s) <= endSegments.get(s),
                             "Segment offset in startStreamCut should be <= segment offset in endStreamCut."));
    }
}
