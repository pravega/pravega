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
import io.pravega.client.batch.SegmentInputSplit;
import io.pravega.client.segment.impl.Segment;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

/**
 * Class to represent a Segment split information.
 */
@Beta
@Data
@Builder
public class SegmentInputSplitImpl implements SegmentInputSplit {

    /**
     * Segment to which the metadata relates to.
     */
    @NonNull
    private final Segment segment;

    /**
     * Start offset for the segment.
     */
    private final long startOffset;

    /**
     * End offset for the segment.
     */
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
    public SegmentInputSplitImpl asImpl() {
        return this;
    }

    public static SegmentInputSplitImpl.SegmentInputSplitImplBuilder builder() {
        return new SegmentInputSplitImpl.SegmentInputSplitBuilderWithValidation();
    }

    private static class SegmentInputSplitBuilderWithValidation extends SegmentInputSplitImplBuilder {
        @Override
        public SegmentInputSplitImpl build() {
            Preconditions.checkState(super.startOffset <= super.endOffset,
                    "Start offset should be less than end offset.");
            return super.build();
        }
    }
}
