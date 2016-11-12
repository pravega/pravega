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

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Stream properties.
 */
class Stream {
    private String name;
    private StreamConfiguration configuration;

    /**
     * Stores all segments in the stream, ordered by number, which implies that
     * these segments are also ordered in the increasing order of their start times.
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
        int numSegments = configuration.getScalingPolicy().getMinNumSegments();
        double keyRange = 1.0 / numSegments;
        IntStream.range(0, numSegments)
                .forEach(
                        x -> {
                            Segment segment = new Segment(x, 0, Long.MAX_VALUE, x * keyRange, (x + 1) * keyRange);
                            segments.add(segment);
                            currentSegments.add(x);
                        }
                );
    }

    String getName() {
        return this.name;
    }

    synchronized StreamConfiguration getStreamConfiguration() {
        return this.configuration;
    }

    synchronized void setConfiguration(StreamConfiguration configuration) {
        this.configuration = configuration;
    }

    synchronized Segment getSegment(int number) {
        return segments.get(number);
    }

    /**
     * Finds all successors of a given segment, that have exactly one predecessor,
     * and hence can be included in the futures of the given segment.
     *
     * @param segment for which default futures are sought.
     * @return the list of successors of specified segment who have only one predecessor.
     */
    private List<Integer> getDefaultFutures(Segment segment) {
        return segment.getSuccessors().stream()
                .filter(x -> segments.get(x).getPredecessors().size() == 1)
                .collect(Collectors.toList());
    }

    /**
     * @return the list of currently active segments
     */
    synchronized SegmentFutures getActiveSegments() {
        return new SegmentFutures(new ArrayList<>(currentSegments), Collections.emptyMap());
    }

    /**
     * @return the list of segments active at a given timestamp.
     * GetActiveSegments runs in O(n), where n is the total number of segments.
     * It can be improved to O(k + logn), where k is the number of active segments at specified timestamp,
     * using augmented interval tree or segment index..
     * TODO: maintain a augmented interval tree or segment tree index
     */
    synchronized SegmentFutures getActiveSegments(long timestamp) {
        List<Integer> currentSegments = new ArrayList<>();
        Map<Integer, Integer> futureSegments = new HashMap<>();
        int i = 0;
        while (i < segments.size() && timestamp >= segments.get(i).getStart()) {
            if (segments.get(i).getEnd() >= timestamp) {
                Segment segment = segments.get(i);
                currentSegments.add(segment.getNumber());
                // futures is set to all the successors of segment that have this segment as the only predecessor
                getDefaultFutures(segment).stream()
                        .forEach(x -> futureSegments.put(x, segment.getNumber()));
            }
            i++;
        }
        return new SegmentFutures(currentSegments, futureSegments);
    }

    /**
     * @param completedSegments completely read segments.
     * @param positions         current consumer positions.
     * @return new consumer positions including new (current or future) segments that can be read from.
     */
    synchronized List<SegmentFutures> getNextSegments(Set<Integer> completedSegments, List<SegmentFutures> positions) {
        Preconditions.checkNotNull(positions);
        Preconditions.checkArgument(positions.size() > 0);

        // successors of completed segments are interesting, which means
        // some of them may become current, and
        // some of them may become future
        Set<Integer> successors = completedSegments.stream().flatMap(x -> segments.get(x).getSuccessors().stream())
                .collect(Collectors.toSet());

        // a successor that has
        // 1. all its predecessors completed, and
        // 2. it is not completed yet, and
        // 3. it is not current in any of the positions,
        // shall become current and be added to some position
        List<Integer> newCurrents = successors.stream().filter(x ->
                // 1. all its predecessors completed, and
                segments.get(x).getPredecessors().stream().allMatch(y -> completedSegments.contains(y))
                        // 2. it is not completed yet, and
                        && !completedSegments.contains(x)
                        // 3. it is not current in any of the positions
                        && positions.stream().allMatch(z -> !z.getCurrent().contains(x))
        ).collect(Collectors.toList());

        Map<Integer, List<Integer>> newFutures = new HashMap<>();
        successors.stream().forEach(
                x -> {
                    // if x is not completed
                    if (!completedSegments.contains(x)) {
                        // number of predecessors not completed == 1
                        List<Integer> filtered = segments.get(x).getPredecessors().stream().filter(y ->
                                !completedSegments.contains(y)).collect(Collectors.toList());
                        if (filtered.size() == 1) {
                            Integer pendingPredecessor = filtered.get(0);
                            if (newFutures.containsKey(pendingPredecessor)) {
                                newFutures.get(pendingPredecessor).add(x);
                            } else {
                                List<Integer> list = new ArrayList<>();
                                list.add(x);
                                newFutures.put(pendingPredecessor, list);
                            }
                        }
                    }
                }
        );

        return divideSegments(newCurrents, newFutures, positions);
    }

    private List<SegmentFutures> divideSegments(List<Integer> newCurrents, Map<Integer, List<Integer>> newFutures,
                                                List<SegmentFutures> positions) {
        List<SegmentFutures> newPositions = new ArrayList<>(positions.size());

        int quotient = newCurrents.size() / positions.size();
        int remainder = newCurrents.size() % positions.size();
        int counter = 0;
        for (int i = 0; i < positions.size(); i++) {
            SegmentFutures position = positions.get(i);

            // add the new current segments
            List<Integer> newCurrent = new ArrayList<>(position.getCurrent());
            int portion = (i < remainder) ? quotient + 1 : quotient;
            for (int j = 0; j < portion; j++, counter++) {
                newCurrent.add(newCurrents.get(counter));
            }
            Map<Integer, Integer> newFuture = new HashMap<>(position.getFutures());
            // add new futures if any
            position.getCurrent().forEach(
                    current -> {
                        if (newFutures.containsKey(current)) {
                            newFutures.get(current).stream().forEach(x -> newFuture.put(x, current));
                        }
                    }
            );
            // add default futures for new and old current segments, if any
            newCurrent.stream().forEach(
                    x -> getDefaultFutures(segments.get(x)).stream()
                            .forEach(
                                    y -> {
                                        if (!newFuture.containsKey(y)) {
                                            newFuture.put(y, x);
                                        }
                                    }
                            )
            );
            newPositions.add(new SegmentFutures(newCurrent, newFuture));
        }
        return newPositions;
    }

    /**
     * Seals a set of segments, and adds a new set of segments as current segments.
     * It sets appropriate endtime and successors of sealed segment.
     *
     * @param sealedSegments segments to be sealed
     * @param keyRanges      new segments to be added as active segments
     * @param scaleTimestamp scaling timestamp. This will be the end time of sealed segments and start time of new
     *                       segments.
     * @return the list of new segments.
     */
    synchronized List<Segment> scale(List<Integer> sealedSegments, List<SimpleEntry<Double, Double>> keyRanges, long
            scaleTimestamp) {
        Preconditions.checkNotNull(sealedSegments);
        Preconditions.checkNotNull(keyRanges);
        Preconditions.checkArgument(sealedSegments.size() > 0);
        Preconditions.checkArgument(keyRanges.size() > 0);

        List<List<Integer>> predecessors = new ArrayList<>();
        for (int i = 0; i < keyRanges.size(); i++) {
            predecessors.add(new ArrayList<>());
        }

        int start = segments.size();
        // assign status, end times, and successors to sealed segments.
        // assign predecessors to new segments
        for (Integer sealed : sealedSegments) {
            Segment segment = segments.get(sealed);
            List<Integer> successors = new ArrayList<>();

            for (int i = 0; i < keyRanges.size(); i++) {
                if (segment.overlaps(keyRanges.get(i).getKey(), keyRanges.get(i).getValue())) {
                    successors.add(start + i);
                    predecessors.get(i).add(sealed);
                }
            }
            Segment sealedSegment = new Segment(sealed, segment.getStart(), scaleTimestamp, segment.getKeyStart(),
                    segment.getKeyEnd(), Segment.Status.Sealed, successors, segment.getPredecessors());
            segments.set(sealed, sealedSegment);
            currentSegments.remove(sealed);
        }

        List<Segment> newSegments = new ArrayList<>();
        // assign start times, numbers to new segments. Add them to segments list and current list.
        for (int i = 0; i < keyRanges.size(); i++) {
            int number = start + i;
            Segment segment = new Segment(number, scaleTimestamp, Long.MAX_VALUE, keyRanges.get(i).getKey(),
                    keyRanges.get(i).getValue(), Segment.Status.Active, new ArrayList<>(), predecessors.get(i));
            newSegments.add(segment);
            segments.add(segment);
            currentSegments.add(number);
        }

        return newSegments;
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
