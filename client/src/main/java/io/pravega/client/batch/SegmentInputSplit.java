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

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;

/**
 * Class to represent a Segment split information.
 */
@Beta
@Data
@Builder
public class SegmentInputSplit {

    /**
     * Segment to which the metadata relates to.
     */
    @NonNull
    @Setter(AccessLevel.NONE)
    private final Segment segment;

    /**
     * Start offset for the segment.
     */
    @Setter(AccessLevel.NONE)
    private final long startOffset;

    /**
     * End offset for the segment.
     */
    @Setter(AccessLevel.NONE)
    private final long endOffset;

    public static SegmentInputSplit.SegmentInputSplitBuilder builder() {
        return new SegmentInputSplit.SegmentInputSplitBuilderWithValidation();
    }

    private static class SegmentInputSplitBuilderWithValidation extends SegmentInputSplitBuilder {
        @Override
        public SegmentInputSplit build() {
            Preconditions.checkState(super.startOffset <= super.endOffset);
            return super.build();
        }
    }
}
