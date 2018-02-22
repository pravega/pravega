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
import io.pravega.client.segment.impl.Segment;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Beta
@Data
@Builder
public class SegmentMetadata {

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

    //TODO: 1. validate that endOffset > startOffset.
}
