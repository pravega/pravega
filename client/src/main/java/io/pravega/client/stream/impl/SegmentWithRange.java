/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
@EqualsAndHashCode(of = "segment")
public class SegmentWithRange {    
    @NonNull
    private final Segment segment;
    private final Range range;

    public SegmentWithRange(Segment segment, double rangeLow, double rangeHigh) {
        this(segment, new Range(rangeLow, rangeHigh));
    }
    
    public SegmentWithRange(Segment segment, Range range) {
        Preconditions.checkNotNull(segment);
        if (range != null) {
            Preconditions.checkArgument(range.low >= 0.0 && range.low <= 1.0);
            Preconditions.checkArgument(range.high >= 0.0 && range.high <= 1.0);
            Preconditions.checkArgument(range.low <= range.high);
        }
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
        
        public boolean overlapsWith(Range other) {
            if (high <= other.low || low >= other.high) {
                return false;
            }
            return true;
        }
    }
    
    public io.pravega.shared.watermarks.SegmentWithRange convert() {
        return new io.pravega.shared.watermarks.SegmentWithRange(getSegment().getSegmentId(), getRange().low, getRange().high);
    }
    
}
