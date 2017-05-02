/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.client.stream;

import com.google.common.base.Preconditions;
import lombok.Data;
import lombok.NonNull;

import java.io.Serializable;

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
        this.segment = segment;
        this.low = rangeLow;
        this.high = rangeHigh;
    }
}
