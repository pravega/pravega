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
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.stream.StreamConfiguration;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Stream properties
 */
class Stream {
    private String name;
    private StreamConfiguration configuration;

    /**
     * Stores all segments in the stream, ordered by number, which implies that
     * these segments are also ordered in the increaing order of their start times.
     * Segment number is the index of that segment in this list.
     */
    private List<Segment> segments;

    /**
     * Stores segment numbers of currently active segments in the stream.
     * It enables efficient access to current segments needed by producers and tailing consumers.
     */
    private List<Integer> currentSegments;

    Stream(String name, StreamConfiguration configuration) {
        this.name = name;
        this.configuration = configuration;
        currentSegments = new ArrayList<>();
        segments = new ArrayList<>();
    }

    String getName() {
        return this.name;
    }

    StreamConfiguration getStreamConfiguration() {
        return this.configuration;
    }

    void setConfiguration(StreamConfiguration configuration) {
        this.configuration = configuration;
    }

    Segment getSegment(int number) {
        return segments.get(number);
    }

    /**
     * Adds a new active segment to the store, with smallest number higher than that of existing segments.
     * End time is assumed to be Max_Value, and successors null, since it is an active segment.
     */
    Segment addActiveSegment(long start, double keyStart, double keyEnd, List<Integer> predecessors) {
        int number = segments.size();
        predecessors.stream().forEach(x -> Preconditions.checkArgument(0 <= x && x <= number - 1));
        Segment segment = new Segment(number, start, Long.MAX_VALUE, keyStart, keyEnd, predecessors, null);
        currentSegments.add(segment.getNumber());
        segments.add(segment);
        return segment;
    }

    /**
     * Adds a new active segment to the store, with smallest number higher than that of existing segments.
     * End time is assumed to be Max_Value, and successors null, since it is an active segment.
     */
    Segment addActiveSegment(Segment segment) {
        Preconditions.checkNotNull(segment);
        Preconditions.checkState(segment.getEnd() == Long.MAX_VALUE);
        segment.setNumber(segments.size());
        segment.setSuccessors(null);
        currentSegments.add(segment.getNumber());
        segments.add(segment);
        return segment;
    }

    /**
     * @return the list of currently active segments
     */
    SegmentFutures getActiveSegments() {
        return new SegmentFutures(Collections.unmodifiableList(currentSegments), null);
    }

    /**
     * @return the list of segments active at a given timestamp.
     * GetActiveSegments runs in O(n), where n is the total number of segments.
     * It can be improved to O(k + logn), where k is the number of active segments at specified timestamp,
     * using augmented interval tree or segment index..
     * TODO: maintain a augmented interval tree or segment tree index
     */
    SegmentFutures getActiveSegments(long timestamp) {
        List<Integer> currentSegments = new ArrayList<>();
        Map<Integer, Integer> futureSegments = new HashMap<>();
        int i = 0;
        while (i < segments.size() && timestamp >= segments.get(i).getStart()) {
            if (segments.get(i).getEnd() >= timestamp) {
                Segment segment = segments.get(i);
                // futures is set to all the successors of segment that have this segment as the only predecessor
                List<Integer> futures = segment.getSuccessors().stream()
                        .filter(x -> segments.get(x).getPredecessors().size() == 1)
                        .collect(Collectors.toList());
                currentSegments.add(segment.getNumber());
                futures.forEach(x -> futureSegments.put(x, segment.getNumber()));
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
    List<SegmentFutures> getNextSegments(List<Integer> completedSegments, List<SegmentFutures> positions) {
        Preconditions.checkNotNull(positions);
        Preconditions.checkArgument(positions.size() > 0);
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    /**
     * Seals a set of segments, and adds a new set of segments as current segments.
     * It sets appropriate endtime and successors of sealed segment.
     * @param sealedSegments segments to be sealed
     * @param newSegments    new segments to be added as active segments
     * @param scaleTimestamp scaling timestamp. This will be the end time of sealed segments and start time of new segments.
     * @return the list of new segments.
     */
    List<Segment> scale(List<Segment> sealedSegments, List<Segment> newSegments, long scaleTimestamp) {
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
