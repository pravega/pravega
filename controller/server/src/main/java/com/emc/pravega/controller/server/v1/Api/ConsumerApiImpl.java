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

import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.SegmentFutures;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.stream.PositionInternal;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.ControllerApi;
import com.emc.pravega.stream.SegmentUri;
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

public class ConsumerApiImpl implements ControllerApi.Consumer {

    private final StreamMetadataStore streamStore;
    private final HostControllerStore hostStore;

    public ConsumerApiImpl(StreamMetadataStore streamStore, HostControllerStore hostStore) {
        this.streamStore = streamStore;
        this.hostStore = hostStore;
    }

    @Override
    public CompletableFuture<SegmentUri> getURI(String stream, int segmentNumber) {
        return CompletableFuture.supplyAsync(() -> SegmentHelper.getSegmentUri(stream, segmentNumber, hostStore));
    }

    /**con
     *
     * @param stream input stream
     * @param timestamp timestamp at which the active position is sought
     * @param n maximum number of positions to be returned
     * @return at most n positions at specified timestamp from the specified stream
     */
    @Override
    public CompletableFuture<List<PositionInternal>> getPositions(String stream, long timestamp, int n) {
        // first fetch segments active at specified timestamp from the specified stream
        CompletableFuture<SegmentFutures> segmentFutures = streamStore.getActiveSegments(stream, timestamp);

        // divide current segments in segmentFutures into at most n positions
        return segmentFutures.thenApply(value -> shard(stream, value, n));
    }

    /**
     * This method divides the current segments from the segmentFutures into at most n positions. It appropriately
     * distributes the future segments in segmentFutures among the shards. E.g., if n=5, and segmentFutures contains
     * a) 3 current segments, then 3 positions will be created each having one current segment
     * b) 6 current segments, then 5 positions will be created 1st position containing #1, #2 current segments
     *    and remaining positions having 1 current segment each
     * @param stream input stream
     * @param segmentFutures input segmentFutures
     * @param n number of shards
     * @return the list of position objects
     */
    private List<PositionInternal> shard(String stream, SegmentFutures segmentFutures, int n) {
        // divide the active segments equally into at most n partition
        int currentCount = segmentFutures.getCurrent().size();
        int quotient = currentCount / n;
        int remainder = currentCount % n;
        // if quotient < 1 then remainder number of positions shall be created, other wise n positions shall be created
        int size = (quotient < 1) ? remainder : n;
        List<PositionInternal> positions = new ArrayList<>(size);

        ListMultimap<Integer, Integer> inverse = Multimaps.invertFrom(
                Multimaps.forMap(segmentFutures.getFutures()),
                ArrayListMultimap.create());

        int counter = 0;
        // create a position object in each iteration of the for loop
        for (int i = 0; i < size; i++) {
            int j = (i < remainder) ? quotient + 1 : quotient;
            List<SegmentId> current = new ArrayList<>(j);
            for (int k = 0; k < j; k++, counter++) {
                Integer number = segmentFutures.getCurrent().get(counter);
                SegmentId segmentId = SegmentHelper.getSegment(stream, number, -1);
                current.add(segmentId);
            }

            // Compute the current and future segments set for position i
            Map<SegmentId, Long> currentSegments = new HashMap<>();
            Map<SegmentId, Long> futureSegments = new HashMap<>();
            current.stream().forEach(
                    x -> {
                        // TODO fetch correct offset within the segment at specified timestamp by contacting pravega host
                        // put it in the currentSegments
                        currentSegments.put(x, 0L);

                        // update futures with all segments in segmentFutures.getFutures having x.number as the predecessor
                        // these segments can be found from the inverted segmentFutures.getFutures
                        int previous = x.getNumber();
                        if (inverse.containsKey(previous)) {
                            inverse.get(previous).stream().forEach(
                                    y -> {
                                        SegmentId segmentId = SegmentHelper.getSegment(stream, y, previous);
                                        futureSegments.put(segmentId, 0L);
                                    }
                            );
                        }
                    }
            );
            // create a new position object with current and futures segments thus computed
            PositionInternal position = new PositionImpl(currentSegments, futureSegments);
            positions.add(position);
        }
        return positions;
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
    public CompletableFuture<List<PositionInternal>> updatePositions(String stream, List<PositionInternal> positions) {
        // initialize completed segments set from those found in the list of input position objects
        Set<Integer> completedSegments = positions.stream().flatMap(x -> x.getCompletedSegments().stream().map(SegmentId::getNumber)).collect(Collectors.toSet());
        Map<Integer, Long> segmentOffsets = new HashMap<>();

        // convert positions to segmentFutures, while updating completedSegments set and
        // storing segment offsets in segmentOffsets map
        List<SegmentFutures> segmentFutures = convertPositionsToSegmentFutures(positions, segmentOffsets);

        // fetch updated SegmentFutures from stream metadata

        CompletableFuture<List<SegmentFutures>> updatedSegmentFutures =
                streamStore.getNextSegments(stream, completedSegments, segmentFutures);

        // finally convert SegmentFutures back to position objects
        return updatedSegmentFutures.thenApply(value -> convertSegmentFuturesToPositions(stream, value, segmentOffsets));
    }

    /**
     * This method converts list of positions into list of segmentFutures.
     * While doing so it updates the completedSegments set and stores segment offsets in a map.
     * @param positions input list of positions
     * @param segmentOffsets map of segment number of its offset that shall be populated in this method
     * @return the list of segmentFutures objects
     */
    private List<SegmentFutures> convertPositionsToSegmentFutures(List<PositionInternal> positions, Map<Integer, Long> segmentOffsets) {
        List<SegmentFutures> segmentFutures = new ArrayList<>(positions.size());

        // construct SegmentFutures for each position object.
        for (PositionInternal position: positions) {
            List<Integer> current = new ArrayList<>(position.getOwnedSegments().size());
            Map<Integer, Integer> futures = new HashMap<>();
            position.getOwnedSegmentsWithOffsets().entrySet().stream().forEach(
                    x -> {
                        int number = x.getKey().getNumber();
                        current.add(number);
                        segmentOffsets.put(number, x.getValue());
                    }
            );
            position.getFutureOwnedSegments().stream().forEach(x -> futures.put(x.getNumber(), x.getPrevious()));
            segmentFutures.add(new SegmentFutures(current, futures));
        }
        return segmentFutures;
    }

    private List<PositionInternal> convertSegmentFuturesToPositions(String stream, List<SegmentFutures> segmentFutures, Map<Integer, Long> segmentOffsets) {
        List<PositionInternal> resultPositions = new ArrayList<>(segmentFutures.size());
        segmentFutures.stream().forEach(
                x -> {
                    Map<SegmentId, Long> currentSegments = new HashMap<>();
                    Map<SegmentId, Long> futureSegments = new HashMap<>();
                    x.getCurrent().stream().forEach(
                            y -> currentSegments.put(SegmentHelper.getSegment(stream, y, -1), segmentOffsets.get(y))
                    );
                    x.getFutures().entrySet().stream().forEach(
                            y -> futureSegments.put(SegmentHelper.getSegment(stream, y.getKey(), y.getValue()), 0L)
                    );
                    resultPositions.add(new PositionImpl(currentSegments, futureSegments));
                }
        );
        return resultPositions;
    }
}
