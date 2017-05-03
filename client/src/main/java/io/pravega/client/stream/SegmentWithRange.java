/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
