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
import java.io.Serializable;
import lombok.Data;
import lombok.NonNull;

/**
 * An identifier for a segment of a stream.
 */
@Data
public class SegmentWithRange implements Serializable {
    private static final long serialVersionUID = 1L;
    @NonNull
    private final Segment segment;
    private final double low;
    private final double high;

    public SegmentWithRange(Segment segment, double rangeLow, double rangeHigh) {
        Preconditions.checkNotNull(segment);
        Preconditions.checkArgument(rangeLow >= 0.0 && rangeLow <= 1.0);
        Preconditions.checkArgument(rangeHigh >= 0.0 && rangeHigh <= 1.0);
        Preconditions.checkArgument(rangeLow <= rangeHigh);
        this.segment = segment;
        this.low = rangeLow;
        this.high = rangeHigh;
    }
}
