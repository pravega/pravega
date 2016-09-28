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
import java.util.List;
import java.util.stream.IntStream;

/**
 * Stream properties
 */
class InMemoryStream implements Stream {
    private final String name;
    private StreamConfiguration configuration;

    /**
     * Stores all segments in the stream, ordered by number, which implies that
     * these segments are also ordered in the increaing order of their start times.
     * Segment number is the index of that segment in this list.
     */
    private final List<InMemorySegment> segments = new ArrayList<>();

    /**
     * Stores segment numbers of currently active segments in the stream.
     * It enables efficient access to current segments needed by producers and tailing consumers.
     */
    private final List<Integer> currentSegments = new ArrayList<>();

    InMemoryStream(String name) {
        this.name = name;
    }

    @Override
    public boolean create(StreamConfiguration configuration) {
        this.configuration = configuration;
        int numSegments = configuration.getScalingingPolicy().getMinNumSegments();
        double keyRange = 1.0 / numSegments;
        IntStream.range(0, numSegments)
                .forEach(
                        x -> {
                            InMemorySegment segment = new InMemorySegment(x, 0, Long.MAX_VALUE, x * keyRange, (x + 1) * keyRange);
                            segments.add(segment);
                            currentSegments.add(x);
                        }
                );
        return true;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public synchronized boolean updateConfiguration(StreamConfiguration configuration) {
        this.configuration = configuration;
        return true;
    }

    @Override
    public synchronized StreamConfiguration getConfiguration() {
        return this.configuration;
    }

    @Override
    public synchronized InMemorySegment getSegment(int number) {
        return segments.get(number);
    }

    @Override
    public List<Integer> getSuccessors(int number) {
        return segments.get(number).getSuccessors();
    }

    @Override
    public List<Integer> getPredecessors(int number) {
        return segments.get(number).getPredecessors();
    }

    /**
     * @return the list of currently active segments
     */
    @Override
    public synchronized List<Integer> getActiveSegments() {
        return Collections.unmodifiableList(currentSegments);
    }

    /**
     * @return the list of segments active at a given timestamp.
     * GetActiveSegments runs in O(n), where n is the total number of segments.
     * It can be improved to O(k + logn), where k is the number of active segments at specified timestamp,
     * using augmented interval tree or segment index..
     * TODO: maintain a augmented interval tree or segment tree index
     */
    @Override
    public synchronized List<Integer> getActiveSegments(long timestamp) {
        List<Integer> currentSegments = new ArrayList<>();
        int i = 0;
        while (i < segments.size() && timestamp >= segments.get(i).getStart()) {
            if (segments.get(i).getEnd() >= timestamp) {
                InMemorySegment segment = segments.get(i);
                currentSegments.add(segment.getNumber());
            }
            i++;
        }
        return currentSegments;
    }

    /**
     * Seals a set of segments, and adds a new set of segments as current segments.
     * It sets appropriate endtime and successors of sealed segment.
     * @param sealedSegments segments to be sealed
     * @param keyRanges    new segments to be added as active segments
     * @param scaleTimestamp scaling timestamp. This will be the end time of sealed segments and start time of new segments.
     * @return the list of new segments.
     */
    @Override
    public synchronized List<Segment> scale(List<Integer> sealedSegments, List<SimpleEntry<Double, Double>> keyRanges, long scaleTimestamp) {
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
        for (Integer sealed: sealedSegments) {
            InMemorySegment segment = segments.get(sealed);
            List<Integer> successors = new ArrayList<>();

            for (int i = 0; i < keyRanges.size(); i++) {
                if (segment.overlaps(keyRanges.get(i).getKey(), keyRanges.get(i).getValue())) {
                    successors.add(start + i);
                    predecessors.get(i).add(sealed);
                }
            }
            InMemorySegment sealedSegment = new InMemorySegment(sealed, segment.getStart(), scaleTimestamp, segment.getKeyStart(), segment.getKeyEnd(), InMemorySegment.Status.Sealed, successors, segment.getPredecessors());
            segments.set(sealed, sealedSegment);
            currentSegments.remove(sealed);
        }

        List<Segment> newSegments = new ArrayList<>();
        // assign start times, numbers to new segments. Add them to segments list and current list.
        for (int i = 0; i < keyRanges.size(); i++) {
            int number = start + i;
            InMemorySegment segment = new InMemorySegment(number, scaleTimestamp, Long.MAX_VALUE, keyRanges.get(i).getKey(), keyRanges.get(i).getValue(), InMemorySegment.Status.Active, new ArrayList<>(), predecessors.get(i));
            newSegments.add(segment);
            segments.add(segment);
            currentSegments.add(number);
        }

        return newSegments;
    }

    public String toString() {
        return String.format("Current Segments:%s\nSegments:%s\n", currentSegments.toString(), segments.toString());
    }
}
