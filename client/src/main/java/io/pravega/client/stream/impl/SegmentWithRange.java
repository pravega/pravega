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

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;

/**
 * An identifier for a segment of a stream.
 */
@Data
@EqualsAndHashCode(of="segment")
public class SegmentWithRange {    
    @NonNull
    private final Segment segment;
    private final Range range;

    public SegmentWithRange(Segment segment, double rangeLow, double rangeHigh) {
        this(segment, new Range(rangeLow, rangeHigh));
    }
    
    public SegmentWithRange(Segment segment, Range range) {
        Preconditions.checkNotNull(segment);
        Preconditions.checkArgument(range.low >= 0.0 && range.low <= 1.0);
        Preconditions.checkArgument(range.high >= 0.0 && range.high <= 1.0);
        Preconditions.checkArgument(range.low <= range.high);
        this.segment = segment;
        this.range = range;
    }
    
    @Data
    public static final class Range {
        private final double low;
        private final double high;
        
        public static Range fromPair(Pair<Double, Double> pair) {
            return new Range(pair.getLeft(), pair.getRight()); 
        }
        
        public Pair<Double, Double> asPair() {
            return Pair.of(low, high);
        }
    }
    
}
