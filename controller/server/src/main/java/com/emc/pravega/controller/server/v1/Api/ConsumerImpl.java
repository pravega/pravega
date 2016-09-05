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

package com.emc.pravega.controller.server.v1.Api;

import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.SegmentFutures;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.impl.PositionImpl;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.Set;
import java.util.stream.Collectors;

public class ConsumerImpl implements ControllerApi.Consumer {

    private StreamMetadataStore streamStore;
    private HostControllerStore hostStore;

    public ConsumerImpl(StreamMetadataStore streamStore, HostControllerStore hostStore) {
        this.streamStore = streamStore;
        this.hostStore = hostStore;
    }

    /**
     *
     * @param stream input stream
     * @param timestamp timestamp at which the active position is sought
     * @param n maximum number of positions to be returned
     * @return at most n positions at specified timestamp from the specified stream
     */
    @Override
    public CompletableFuture<List<Position>> getPositions(String stream, long timestamp, int n) {
        return CompletableFuture.supplyAsync(
                () -> {
                    // fetch the segments active at timestamp from specified stream
                    SegmentFutures segmentFutures = streamStore.getActiveSegments(stream, timestamp);

                    // divide the active segments equally into at most n partitions
                    int currentCount = segmentFutures.getCurrent().size();
                    int quotient = currentCount / n;
                    int remainder = currentCount % n;

                    ListMultimap<Integer, Integer> inverse = Multimaps.invertFrom(
                            Multimaps.forMap(segmentFutures.getFutures()),
                            ArrayListMultimap.create());

                    int size = (quotient < 1) ? remainder : n;
                    List<Position> positions = new ArrayList<>(size);

                    int counter = 0;
                    for (int i = 0; i < size; i++) {
                        int j = (i < remainder) ? quotient + 1 : quotient;
                        List<SegmentId> current = new ArrayList<>(j);
                        for (int k = 0; k < j; k++, counter++) {
                            Integer number = segmentFutures.getCurrent().get(counter);
                            SegmentId segmentId = SegmentHelper.getSegmentId(stream, number, 0, hostStore);
                            current.add(segmentId);
                        }
                        Map<SegmentId, Long> currentSegments = new HashMap<>();
                        Map<SegmentId, Long> futureSegments = new HashMap<>();
                        current.stream().forEach(
                                x -> {
                                    // TODO fetch correct offset within the segment at specified timestamp by contacting pravega host
                                    currentSegments.put(x, 0L);
                                    int previous = x.getNumber();
                                    if (inverse.containsKey(previous)) {
                                        inverse.get(previous).stream().forEach(
                                                y -> {
                                                    SegmentId segmentId = SegmentHelper.getSegmentId(stream, y, previous, hostStore);
                                                    futureSegments.put(segmentId, 0L);
                                                }
                                        );
                                    }
                                }
                        );
                        Position position = new PositionImpl(currentSegments, futureSegments);
                        positions.add(position);
                    }
                    return positions;
                }
        );
    }

    /**
     * Given a list of position objects of a stream, return the updated list of position objects taking into account
     * completely read segments whose successors can possibly become current, or
     * completely read segments whose successors can possibly be added to futures in some positions, or
     * current segments whose successors can possibly be added to futures in their respective position.
     * @param stream input stream
     * @param positions input list of position objects
     * @return the updated list of position objects
     */
    @Override
    public CompletableFuture<List<Position>> updatePositions(String stream, List<Position> positions) {
        return CompletableFuture.supplyAsync(
                () -> {
                    // collect the completed segments from list of position objects
                    Set<Integer> completedSegments = positions.stream().flatMap(x -> x.getCompletedSegments().stream().map(y -> y.getNumber())).collect(Collectors.toSet());
                    Map<Integer, Long> segmentOffsets = new HashMap<>();
                    List<SegmentFutures> segmentFutures = new ArrayList<>(positions.size());

                    // construct SegmentFutures for each position object.
                    for (Position position: positions) {
                        List<Integer> current = new ArrayList<>(position.getOwnedSegments().size());
                        Map<Integer, Integer> futures = new HashMap<>();
                        position.getOwnedSegmentsWithOffsets().entrySet().stream().forEach(
                                x -> {
                                    int number = x.getKey().getNumber();
                                    current.add(number);
                                    Segment segment = streamStore.getSegment(stream, number);
                                    // update completed segments set with implicitly completed segments
                                    segment.getPredecessors().stream().forEach(y -> completedSegments.add(y));
                                    segmentOffsets.put(number, x.getValue());
                                }
                        );
                        position.getFutureOwnedSegments().stream().forEach(
                                x -> {
                                    futures.put(x.getNumber(), x.getPrevious());
                                }
                        );
                        segmentFutures.add(new SegmentFutures(current, futures));
                    }

                    // fetch updated SegmentFutures from stream metadata
                    List<SegmentFutures> result = streamStore.getNextSegments(stream, completedSegments, segmentFutures);

                    // finally convert SegmentFutures back to position objects
                    return getNewPositions(stream, result, segmentOffsets);
                }
        );
    }

    private List<Position> getNewPositions(String stream, List<SegmentFutures> segmentFutures, Map<Integer, Long> segmentOffsets) {
        List<Position> resultPositions = new ArrayList<>(segmentFutures.size());
        segmentFutures.stream().forEach(
                x -> {
                    Map<SegmentId, Long> currentSegments = new HashMap<>();
                    Map<SegmentId, Long> futureSegments = new HashMap<>();
                    x.getCurrent().stream().forEach(
                            y -> currentSegments.put(SegmentHelper.getSegmentId(stream, y, 0, hostStore), segmentOffsets.get(y))
                    );
                    x.getFutures().entrySet().stream().forEach(
                            y -> futureSegments.put(SegmentHelper.getSegmentId(stream, y.getKey(), y.getValue(), hostStore), 0L)
                    );
                    resultPositions.add(new PositionImpl(currentSegments, futureSegments));
                }
        );
        return resultPositions;
    }
}
