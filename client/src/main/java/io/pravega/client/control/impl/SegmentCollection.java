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
package io.pravega.client.control.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.impl.SegmentWithRange;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.NavigableMap;
import lombok.EqualsAndHashCode;

/**
 * Organizes Segments by key ranges.
 */
@EqualsAndHashCode
public abstract class SegmentCollection {

    /**
     * Maps the upper end of a range to the corresponding segment. The range in the value is the
     * range of keyspace the segment has been assigned. The range in the value is NOT the same as
     * the difference between two keys. The keys correspond to the range that the client
     * should route to, where as the one in the value is the range the segment it assigned. These
     * may be different if a client still has a preceding segment in its map. In which case a
     * segment's keys may not contain the full assigned range.
     */
    protected final NavigableMap<Double, SegmentWithRange> segments;

    /**
     * Creates a new instance of the SegmentCollection class.
     *
     * @param segments Segments keyed by the largest key in their key range.
     *                 i.e. If there are two segments split evenly, the first should have a value of 0.5 and the second 1.0.
     */
    public SegmentCollection(NavigableMap<Double, SegmentWithRange> segments) {
        this.segments = Collections.unmodifiableNavigableMap(segments);
        verifySegments();
    }

    private void verifySegments() {
        if (!segments.isEmpty()) {
            Preconditions.checkArgument(segments.firstKey() > 0.0, "Nonsense value for segment.");
            Preconditions.checkArgument(segments.lastKey() >= 1.0, "Last segment missing.");
            Preconditions.checkArgument(segments.lastKey() < 1.00001, "Segments should only go up to 1.0");
        }
    }

    protected abstract double hashToRange(String key);

    public Segment getSegmentForKey(String key) {
        return getSegmentForKey(hashToRange(key));
    }

    public Segment getSegmentForKey(double key) {
        Preconditions.checkArgument(key >= 0.0);
        Preconditions.checkArgument(key <= 1.0);
        return segments.ceilingEntry(key).getValue().getSegment();
    }

    public Collection<Segment> getSegments() {
        ArrayList<Segment> result = new ArrayList<>(segments.size());
        for (SegmentWithRange seg : segments.values()) {
            result.add(seg.getSegment());
        }
        return result;
    }

    @Override
    public String toString() {
        return "StreamSegments:" + segments.toString();
    }
}
