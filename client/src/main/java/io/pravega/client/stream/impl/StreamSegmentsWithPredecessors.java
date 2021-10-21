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

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The successor segments of a given segment.
 */
@EqualsAndHashCode
@ToString(exclude = "delegationToken")
public class StreamSegmentsWithPredecessors {
    private final Map<SegmentWithRange, List<Long>> segmentWithPredecessors;
    private final Map<Long, List<SegmentWithRange>> replacementRanges;
    @Getter
    private final String delegationToken;

    public StreamSegmentsWithPredecessors(final Map<SegmentWithRange, List<Long>> segments, String delegationToken) {
        this.segmentWithPredecessors = ImmutableMap.copyOf(segments);

        Map<Long, List<SegmentWithRange>> replacementRanges = new HashMap<>();
        for (Entry<SegmentWithRange, List<Long>> entry : segments.entrySet()) {
            for (Long oldSegment : entry.getValue()) {
                List<SegmentWithRange> newRanges = replacementRanges.get(oldSegment);
                if (newRanges == null) {
                    newRanges = new ArrayList<>(2);
                    replacementRanges.put(oldSegment, newRanges);
                }
                newRanges.add(entry.getKey());
            }
        }
        this.replacementRanges = Collections.unmodifiableMap(replacementRanges);
        this.delegationToken = delegationToken;
    }

    /**
     * Get Segment to Predecessor mapping.
     *
     * @return A {@link Map} with {@link SegmentWithRange} as key and {@link List} of {@link Integer} as value.
     */
    public Map<SegmentWithRange, List<Long>> getSegmentToPredecessor() {
        return segmentWithPredecessors;
    }

    /**
     * Returns a map of the segment numbers to segment/ranges. The segment numbers (keys) comprise
     * the predecessor segments, while the segment/ranges comprise the successor segments and their
     * corresponding ranges.
     * 
     * @return Predecessors mapped to successors.
     */
    public Map<Long, List<SegmentWithRange>> getReplacementRanges() {
        return replacementRanges;
    }

}
