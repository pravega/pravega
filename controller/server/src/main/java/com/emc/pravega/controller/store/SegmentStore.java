/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.store;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * In-memory segment store of a stream
 */
public class SegmentStore {
    /**
     * Stores all segments in the stream, ordered by number, which implies that
     * these segments are also ordered in the increaing order of their start times.
     * Segment number is the index of that segment in this list.
     */
    List<Segment> segments;

    /**
     * Stores segment numbers of currently active segments in the stream.
     * It enables efficient access to current segments by producers and tailing consumers.
     */
    List<Integer> currentSegments;

    public void initialize() {
        // TODO: initialize from persistent store, create arrays of appropriate size
        currentSegments = new ArrayList<>();
        segments = new ArrayList<>();
    }

    public Segment getSegment(int number) {
        return segments.get(number);
    }

    /**
     * Adds a new active segment to the store, with smallest number higher than that of existing segments.
     * End time is assumed to be Max_Value, and successors null, since it is an active segment.
     */
    public void addActiveSegment(long start, double keyStart, double keyEnd, List<Integer> predecessors) {
        int number = segments.size();
        predecessors.stream().forEach(x -> Preconditions.checkArgument(0 <= x && x <= number - 1));
        Segment segment = new Segment(number, start, Long.MAX_VALUE, keyStart, keyEnd, predecessors, null);
        currentSegments.add(segment.number);
        segments.add(segment);
    }

    /**
     * Adds a new active segment to the store, with smallest number higher than that of existing segments.
     * End time is assumed to be Max_Value, and successors null, since it is an active segment.
     */
    public void addActiveSegment(Segment segment) {
        Preconditions.checkNotNull(segment);
        Preconditions.checkState(segment.end == Long.MAX_VALUE);
        segment.number = segments.size();
        segment.successors = null;
        currentSegments.add(segment.number);
        segments.add(segment);
    }

    /**
     * @return the list of currently active segments
     */
    public SegmentFutures getActiveSegments() {
        return new SegmentFutures(Collections.unmodifiableList(currentSegments), null);
    }

    /**
     * @return the list of segments active at a given timestamp.
     * GetActiveSegments runs in O(n), where n is the total number of segments.
     * It will improve to O(k * logn), where k is the number of active segments at timestamp,
     * using augmented interval tree or segment index.
     * TODO: maintain a augmented interval tree or segment tree index
     */
    public SegmentFutures getActiveSegments(long timestamp) {
        List<Integer> currentSegments = new ArrayList<>();
        Map<Integer, Integer> futureSegments = new HashMap<>();
        int i = 0;
        while (i < segments.size() && timestamp >= segments.get(i).start) {
            if (segments.get(i).end >= timestamp) {
                Segment segment = segments.get(i);
                List<Integer> futures = segment.successors.stream()
                        .filter(x -> segments.get(x).predecessors.size() == 1)
                        .collect(Collectors.toList());
                currentSegments.add(segment.number);
                futures.forEach(x -> futureSegments.put(x, segment.number));
            }
            i++;
        }
        return new SegmentFutures(currentSegments, futureSegments);
    }

    /**
     * @param completedSegments completely read segments.
     * @param positions current consumer positions.
     * @return new consumer positions including new (current or future) segments that can be read from.
     */
    public List<SegmentFutures> getNextPositions(List<Integer> completedSegments, List<SegmentFutures> positions) {
        Preconditions.checkNotNull(positions);
        Preconditions.checkArgument(positions.size() > 0);
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public void scale(List<Segment> sealedSegments, List<Segment> newSegments, long scaleTimestamp) {
        Preconditions.checkNotNull(sealedSegments);
        Preconditions.checkNotNull(newSegments);
        Preconditions.checkArgument(sealedSegments.size() > 0);
        Preconditions.checkArgument(newSegments.size() > 0);
        // TODO: assign status, end times, and successors to sealed segments.
        // TODO: assign predecessors, start times, numbers to new segments. Add them to segments list and current list.
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Current Segments:\n");
        sb.append(currentSegments.toString());
        sb.append("Segments:\n");
        sb.append(segments.toString());
        return sb.toString();
    }
}
