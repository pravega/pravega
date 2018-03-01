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

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.segment.impl.Segment;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

/**
 * Implementation of {@link SegmentRange}.
 */
@Beta
@Builder
@ToString
public class SegmentRangeImpl implements SegmentRange {

    /**
     * Segment to which the metadata relates to.
     */
    @NonNull
    @Getter(value = AccessLevel.PACKAGE)
    private final Segment segment;

    /**
     * Start offset for the segment.
     */
    @Getter(value = AccessLevel.PACKAGE)
    private final long startOffset;

    /**
     * End offset for the segment.
     */
    @Getter(value = AccessLevel.PACKAGE)
    private final long endOffset;

    @Override
    public int getSegmentNumber() {
        return segment.getSegmentNumber();
    }

    @Override
    public String getStreamName() {
        return segment.getStreamName();
    }

    @Override
    public String getScopeName() {
        return segment.getScopedName();
    }

    @Override
    public SegmentRangeImpl asImpl() {
        return this;
    }

    public static SegmentRangeImpl.SegmentRangeImplBuilder builder() {
        return new SegmentRangeBuilderWithValidation();
    }

    private static class SegmentRangeBuilderWithValidation extends SegmentRangeImplBuilder {
        @Override
        public SegmentRangeImpl build() {
            Preconditions.checkState(super.startOffset <= super.endOffset,
                    "Start offset should be less than end offset.");
            return super.build();
        }
    }
}
