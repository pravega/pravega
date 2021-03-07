/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.batch.impl;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.StreamCut;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Implementation of {@link SegmentRange}.
 */

@Beta
@Builder
@ToString
@EqualsAndHashCode
public class SegmentRangeImpl implements SegmentRange {
    private static final long serialVersionUID = 1L;

    /**
     * Segment to which the metadata relates to.
     */
    @NonNull
    @Getter(value = AccessLevel.PACKAGE)
    private final Segment segment;

    /**
     * Start offset for the segment.
     */
    @Getter
    private final long startOffset;

    /**
     * End offset for the segment.
     */
    @Getter
    private final long endOffset;

    @Override
    public long getSegmentId() {
        return segment.getSegmentId();
    }

    @Override
    public String getStreamName() {
        return segment.getStreamName();
    }

    @Override
    public String getScope() {
        return segment.getScope();
    }

    @Override
    public SegmentRangeImpl asImpl() {
        return this;
    }

    public static final class SegmentRangeImplBuilder {
        public SegmentRangeImpl build() {
            Preconditions.checkState(startOffset <= endOffset, "Start offset should be less than end offset.");
            return new SegmentRangeImpl(segment, startOffset, endOffset);
        }
    }

    /**
     * Obtain {@link SegmentRange} from start and end {@link StreamCut} for a given {@link Segment}.
     * @param segment The {@link Segment}
     * @param start Beginning of the {@link SegmentRange}
     * @param end End of the {@link SegmentRange}
     * @return {@link SegmentRange} covering start-end
     */
    public static SegmentRange fromStreamCuts(final Segment segment, final StreamCut start, final StreamCut end) {
        Preconditions.checkState(!start.equals(StreamCut.UNBOUNDED), "Start StreamCut may not be UNBOUNDED");
        Preconditions.checkState(!end.equals(StreamCut.UNBOUNDED), "End StreamCut may not be UNBOUNDED");
        long startOffset = start.asImpl().getPositions().getOrDefault(segment, -1L);
        long endOffset = end.asImpl().getPositions().getOrDefault(segment, -1L);
        Preconditions.checkState(startOffset >= 0 && endOffset > startOffset, "Start offset should be less than end offset.");
        return new SegmentRangeImpl(segment, startOffset, endOffset);
    }
}
