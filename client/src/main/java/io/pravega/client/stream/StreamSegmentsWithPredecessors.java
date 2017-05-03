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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;

/**
 * The successor segments of a given segment.
 */
@EqualsAndHashCode
public class StreamSegmentsWithPredecessors {
    private final Map<Segment, List<Integer>> segmentWithPredecessors;
    private final Map<Integer, List<SegmentWithRange>> replacementRanges;

    public StreamSegmentsWithPredecessors(final Map<SegmentWithRange, List<Integer>> segments) {
        segmentWithPredecessors = Collections.unmodifiableMap(segments.entrySet().stream().collect(
                Collectors.toMap(entry -> entry.getKey().getSegment(), Map.Entry::getValue)));

        Map<Integer, List<SegmentWithRange>> replacementRanges = new HashMap<>();
        for (Entry<SegmentWithRange, List<Integer>> entry : segments.entrySet()) {
            for (Integer oldSegment : entry.getValue()) {
                List<SegmentWithRange> newRanges = replacementRanges.get(oldSegment);
                if (newRanges == null) {
                    newRanges = new ArrayList<>(2);
                    replacementRanges.put(oldSegment, newRanges);
                }
                newRanges.add(entry.getKey());
            }
        }
        this.replacementRanges = Collections.unmodifiableMap(replacementRanges);
    }

    /**
     * Get Segment to Predecessor mapping.
     *
     * @return A {@link Map} with {@link Segment} as key and {@link List} of {@link Integer} as value.
     */
    public Map<Segment, List<Integer>> getSegmentToPredecessor() {
        return segmentWithPredecessors;
    }

    /**
     * Returns a map of the segment numbers to segment/ranges. The segment numbers (keys) comprise
     * the predecessor segments, while the segment/ranges comprise the successor segments and their
     * corresponding ranges.
     * 
     * @return Predecessors mapped to successors.
     */
    public Map<Integer, List<SegmentWithRange>> getReplacementRanges() {
        return replacementRanges;
    }

}
