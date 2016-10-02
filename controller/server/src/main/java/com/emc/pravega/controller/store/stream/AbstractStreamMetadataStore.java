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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Abstract Stream metadata store. It implements various read queries using the Stream interface.
 * Create and update queries are delegated to the specific implementations of this abstract class.
 */
public abstract class AbstractStreamMetadataStore implements StreamMetadataStore {

    public abstract Stream getStream(String name);

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
        return constructSegmentFutures(stream, activeSegments);
    }

    @Override
    public List<SegmentFutures> getNextSegments(String name, Set<Integer> completedSegments, List<SegmentFutures> positions) {
        Preconditions.checkNotNull(positions);
        Preconditions.checkArgument(positions.size() > 0);

        Stream stream = getStream(name);

        Set<Integer> current = new HashSet<>();
        positions.forEach(position ->
                position.getCurrent().forEach(current::add));

        Set<Integer> implicitCompletedSegments = getImplicitCompletedSegments(stream, completedSegments, current);

        Set<Integer> successors = getSuccessors(stream, implicitCompletedSegments);

        List<Integer> newCurrents = getNewCurrents(stream, successors, implicitCompletedSegments, positions);

        Map<Integer, List<Integer>> newFutures = getNewFutures(stream, successors, implicitCompletedSegments);

        return divideSegments(stream, newCurrents, newFutures, positions);
    }

    @Override
    public List<Segment> scale(String name, List<Integer> sealedSegments, List<AbstractMap.SimpleEntry<Double, Double>> newRanges, long scaleTimestamp) {
        return getStream(name).scale(sealedSegments, newRanges, scaleTimestamp);
    }

    private SegmentFutures constructSegmentFutures(Stream stream, List<Integer> activeSegments) {
        Map<Integer, Integer> futureSegments = new HashMap<>();
        for (Integer number: activeSegments) {
            for (Integer future : getDefaultFutures(stream, number)) {
                futureSegments.put(future, number);
            }
        }
        return new SegmentFutures(activeSegments, futureSegments);
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

    private Set<Integer> getImplicitCompletedSegments(Stream stream, Set<Integer> completedSegments, Set<Integer> current) {
        // append completed segment set with implicitly completed segments
        current.stream().forEach(number ->
                stream.getPredecessors(number).stream().forEach(completedSegments::add));
        return completedSegments;
    }

    private Set<Integer> getSuccessors(Stream stream, Set<Integer> completedSegments) {
        // successors of completed segments are interesting, which means
        // some of them may become current, and
        // some of them may become future
        //Set<Integer> successors = completedSegments.stream().flatMap(x -> stream.getSuccessors(x).stream()).collect(Collectors.toSet());
        return completedSegments.stream().flatMap(x -> stream.getSuccessors(x).stream()).collect(Collectors.toSet());
    }

    private List<Integer> getNewCurrents(Stream stream, Set<Integer> successors, Set<Integer> completedSegments, List<SegmentFutures> positions) {
        // a successor that has
        // 1. it is not completed yet, and
        // 2. it is not current in any of the positions,
        // 3. all its predecessors completed, and
        // shall become current and be added to some position
        List<Integer> newCurrents = successors.stream().filter(x ->
                        // 1. it is not completed yet, and
                        !completedSegments.contains(x)
                                // 2. it is not current in any of the positions
                                && positions.stream().allMatch(z -> !z.getCurrent().contains(x))
                                // 1. all its predecessors completed, and
                                && stream.getPredecessors(x).stream().allMatch(completedSegments::contains)
        ).collect(Collectors.toList());
        return newCurrents;
    }

    private Map<Integer, List<Integer>> getNewFutures(Stream stream, Set<Integer> successors, Set<Integer> completedSegments) {

        List<Integer> subset = successors.stream().filter(x -> !completedSegments.contains(x)).collect(Collectors.toList());

        List<List<Integer>> preds =
                subset.stream()
                        .map(number -> stream.getPredecessors(number).stream()
                                .filter(y -> !completedSegments.contains(y)).collect(Collectors.toList()))
                        .collect(Collectors.toList());

        Map<Integer, List<Integer>> map = new HashMap<>();
        for (int i = 0; i < preds.size(); i++) {
            List<Integer> filtered = preds.get(i);
            if (filtered.size() == 1) {
                Integer pendingPredecessor = filtered.get(0);
                if (map.containsKey(pendingPredecessor)) {
                    map.get(pendingPredecessor).add(subset.get(i));
                } else {
                    List<Integer> list = new ArrayList<>();
                    list.add(subset.get(i));
                    map.put(pendingPredecessor, list);
                }
            }
        }
        return map;
    }

    /**
     * Divides the set of new current segments among existing positions and returns the updated positions
     * @param stream stream
     * @param newCurrents new set of current segments
     * @param newFutures new set of future segments
     * @param positions positions to be updated
     * @return the updated sequence of positions
     */
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
