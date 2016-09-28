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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract Stream metadata store. It implements most of the operaions on stream object.
 * It is upto the concrete implementation
 */
public abstract class AbstractStreamMetadataStore implements StreamMetadataStore {

    public abstract Stream getStream(String name);

    @Override
    public abstract boolean createStream(String name, StreamConfiguration configuration);

    @Override
    public boolean updateConfiguration(String name, StreamConfiguration configuration) {
        return getStream(name).updateConfiguration(configuration);
    }

    @Override
    public StreamConfiguration getConfiguration(String name) {
        return getStream(name).getConfiguration();
    }

    @Override
    public Segment getSegment(String name, int number) {
        return getStream(name).getSegment(number);
    }

    @Override
    public SegmentFutures getActiveSegments(String name) {
        List<Integer> currentSegments = getStream(name).getActiveSegments();
        return new SegmentFutures(new ArrayList<>(currentSegments), Collections.EMPTY_MAP);
    }

    @Override
    public SegmentFutures getActiveSegments(String name, long timestamp) {
        Stream stream = getStream(name);
        List<Integer> activeSegments = stream.getActiveSegments(timestamp);
        List<Integer> currentSegments = new ArrayList<>();
        Map<Integer, Integer> futureSegments = new HashMap<>();
        int i = 0;
        while (i < activeSegments.size()) {
            int number = activeSegments.get(i);
            currentSegments.add(number);
            // futures is set to all the successors of segment that have this segment as the only predecessor
            getDefaultFutures(stream, number).stream()
                    .forEach(x -> futureSegments.put(x, number));
            i++;
        }
        return new SegmentFutures(currentSegments, futureSegments);
    }

    @Override
    public List<SegmentFutures> getNextSegments(String name, Set<Integer> completedSegments, List<SegmentFutures> positions) {
        Preconditions.checkNotNull(positions);
        Preconditions.checkArgument(positions.size() > 0);

        Stream stream = getStream(name);

        // append completed segment set with implicitly completed segments
        positions.forEach(position ->
                position.getCurrent().forEach(number ->
                        stream.getPredecessors(number).stream().forEach(completedSegments::add)));

        // successors of completed segments are interesting, which means
        // some of them may become current, and
        // some of them may become future
        Set<Integer> successors = completedSegments.stream().flatMap(x -> stream.getSuccessors(x).stream()).collect(Collectors.toSet());

        // a successor that has
        // 1. all its predecessors completed, and
        // 2. it is not completed yet, and
        // 3. it is not current in any of the positions,
        // shall become current and be added to some position
        List<Integer> newCurrents = successors.stream().filter(x ->
                        // 1. all its predecessors completed, and
                        stream.getPredecessors(x).stream().allMatch(completedSegments::contains)
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
                        List<Integer> filtered = stream.getPredecessors(x).stream().filter(y -> !completedSegments.contains(y)).collect(Collectors.toList());
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

        return divideSegments(stream, newCurrents, newFutures, positions);
    }

    @Override
    public List<Segment> scale(String name, List<Integer> sealedSegments, List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp) {
        return getStream(name).scale(sealedSegments, newRanges, scaleTimestamp);
    }

    /**
     * Finds all successors of a given segment, that have exactly one predecessor,
     * and hence can be included in the futures of the given segment.
     * @param stream input stream
     * @param number segment number for which default futures are sought.
     * @return the list of successors of specified segment who have only one predecessor.
     */
    private List<Integer> getDefaultFutures(Stream stream, int number) {
        return stream.getSuccessors(number).stream()
                .filter(x -> stream.getPredecessors(x).size() == 1)
                .collect(Collectors.toList());
    }

    private List<SegmentFutures> divideSegments(Stream stream, List<Integer> newCurrents, Map<Integer, List<Integer>> newFutures, List<SegmentFutures> positions) {
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
                    x -> getDefaultFutures(stream, x).stream()
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
}
