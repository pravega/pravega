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

package io.pravega.controller.store.stream;

import lombok.Data;
import lombok.ToString;

import java.util.AbstractMap;

/**
 * Properties of a stream segment that don't change over its lifetime.
 */
@Data
@ToString(includeFieldNames = true)
public class Segment {

    protected final int number;
    protected final long start;
    protected final double keyStart;
    protected final double keyEnd;

    public boolean overlaps(final Segment segment) {
        return segment.getKeyEnd() > keyStart && segment.getKeyStart() < keyEnd;
    }

    public boolean overlaps(final double keyStart, final double keyEnd) {
        return keyEnd > this.keyStart && keyStart < this.keyEnd;
    }

    public static boolean overlaps(final AbstractMap.SimpleEntry<Double, Double> first,
                                   final AbstractMap.SimpleEntry<Double, Double> second) {
        return second.getValue() > first.getKey() && second.getKey() < first.getValue();
    }
}
