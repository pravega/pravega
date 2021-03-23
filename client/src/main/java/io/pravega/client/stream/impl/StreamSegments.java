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
import io.pravega.client.control.impl.SegmentCollection;
import io.pravega.client.segment.impl.Segment;
import io.pravega.common.hash.HashHelper;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

/**
 * The segments that within a stream at a particular point in time.
 */
@EqualsAndHashCode(callSuper = true)
@Slf4j
public class StreamSegments extends SegmentCollection {
    private static final HashHelper HASHER = HashHelper.seededWith("EventRouter");  // DO NOT change this string.

    /**
     * Creates a new instance of the StreamSegments class.
     *
     * @param segments Segments of a stream, keyed by the largest key in their key range.
     *                 i.e. If there are two segments split evenly, the first should have a value of 0.5 and the second 1.0.
     */
    public StreamSegments(NavigableMap<Double, SegmentWithRange> segments) {
        super(segments);
    }

    @Override
    protected double hashToRange(String key) {
        return HASHER.hashToRange(key);
    }

    public int getNumberOfSegments() {
        return segments.size();
    }

    public StreamSegments withReplacementRange(Segment segment, StreamSegmentsWithPredecessors replacementRanges) {
        SegmentWithRange replacedSegment = findReplacedSegment(segment);
        verifyReplacementRange(replacedSegment, replacementRanges);
        NavigableMap<Double, SegmentWithRange> result = new TreeMap<>();
        Map<Long, List<SegmentWithRange>> replacedRanges = replacementRanges.getReplacementRanges();
        List<SegmentWithRange> replacements = replacedRanges.get(segment.getSegmentId());
        Preconditions.checkNotNull(replacements, "Empty set of replacements for: {}", segment.getSegmentId());
        replacements.sort(Comparator.comparingDouble((SegmentWithRange s) -> s.getRange().getHigh()).reversed());
        verifyContinuous(replacements);
        for (Entry<Double, SegmentWithRange> existingEntry : segments.descendingMap().entrySet()) { // iterate from the highest key.
            final SegmentWithRange existingSegment = existingEntry.getValue();
            if (existingSegment.equals(replacedSegment)) { // Segment needs to be replaced.
                // Invariant: The successor segment(s)'s range should be limited to the replaced segment's range, thereby
                // ensuring that newer writes to the successor(s) happen only for the replaced segment's range.
                for (SegmentWithRange segmentWithRange : replacements) {
                    Double lowerBound = segments.lowerKey(existingEntry.getKey()); // Used to skip over items not in the clients view yet.
                    if (lowerBound == null || segmentWithRange.getRange().getHigh() >= lowerBound) { 
                        result.put(Math.min(segmentWithRange.getRange().getHigh(), existingEntry.getKey()), segmentWithRange);
                    }
                }
            } else {
                // update remaining values.
                result.put(existingEntry.getKey(), existingEntry.getValue());
            }
        }
        removeDuplicates(result);
        return new StreamSegments(result);
    }
    
    /**
     * This combines consecutive entries in the map that refer to the same segment.
     * This happens following a merge because the preceding segments are replaced one at a time.
     */
    private void removeDuplicates(NavigableMap<Double, SegmentWithRange> result) {
        Segment last = null;
        for (Iterator<SegmentWithRange> iterator = result.descendingMap().values().iterator(); iterator.hasNext();) {
            SegmentWithRange current = iterator.next();
            if (current.getSegment().equals(last)) {
                iterator.remove();
            }
            last = current.getSegment();
        }
    }

    private SegmentWithRange findReplacedSegment(Segment segment) {
        return segments.values()
                       .stream()
                       .filter(withRange -> withRange.getSegment().equals(segment))
                       .findFirst()
                       .orElseThrow(() -> new IllegalArgumentException("Segment to be replaced should be present in the segment list"));
    }
    
    /**
     * Checks that replacementSegments provided are consistent with the segments that are currently being used.
     * @param replacedSegment The segment on which EOS was reached
     * @param replacementSegments The StreamSegmentsWithPredecessors to verify
     */
    private void verifyReplacementRange(SegmentWithRange replacedSegment, StreamSegmentsWithPredecessors replacementSegments) {
        log.debug("Verification of replacement segments {} with the current segments {}", replacementSegments, segments);
        Map<Long, List<SegmentWithRange>> replacementRanges = replacementSegments.getReplacementRanges();
        List<SegmentWithRange> replacements = replacementRanges.get(replacedSegment.getSegment().getSegmentId());
        Preconditions.checkArgument(replacements != null, "Replacement segments did not contain replacements for segment being replaced");
        if (replacementRanges.size() == 1) {
            //Simple split
            Preconditions.checkArgument(replacedSegment.getRange().getHigh() == getUpperBound(replacements));
            Preconditions.checkArgument(replacedSegment.getRange().getLow() == getLowerBound(replacements));
        } else {
            Preconditions.checkArgument(replacedSegment.getRange().getHigh() <= getUpperBound(replacements));
            Preconditions.checkArgument(replacedSegment.getRange().getLow() >= getLowerBound(replacements));
        }
        for (Entry<Long, List<SegmentWithRange>> ranges : replacementRanges.entrySet()) {
            Entry<Double, SegmentWithRange> upperReplacedSegment = segments.floorEntry(getUpperBound(ranges.getValue()));
            Entry<Double, SegmentWithRange> lowerReplacedSegment = segments.higherEntry(getLowerBound(ranges.getValue()));
            Preconditions.checkArgument(upperReplacedSegment != null, "Missing replaced replacement segments %s",
                                        replacementSegments);
            Preconditions.checkArgument(lowerReplacedSegment != null, "Missing replaced replacement segments %s",
                                        replacementSegments);
        }
    }
    
    private void verifyContinuous(List<SegmentWithRange> newSegments) {
        double previous = newSegments.get(0).getRange().getHigh();
        for (SegmentWithRange s : newSegments) {
            Preconditions.checkArgument(previous == s.getRange().getHigh(), "Replacement segments were not continious: {}", newSegments);
            previous = s.getRange().getLow();
        }
    }

    private double getLowerBound(List<SegmentWithRange> values) {
        double lowerReplacementRange = 1;
        for (SegmentWithRange range : values) {
            lowerReplacementRange = Math.min(lowerReplacementRange, range.getRange().getLow());
        }
        return lowerReplacementRange;
    }

    private double getUpperBound(List<SegmentWithRange> value) {
        double upperReplacementRange = 0;
        for (SegmentWithRange range : value) {
            upperReplacementRange = Math.max(upperReplacementRange, range.getRange().getHigh());
        }
        return upperReplacementRange;
    }
}
