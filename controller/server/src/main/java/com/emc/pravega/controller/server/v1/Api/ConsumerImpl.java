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

import com.emc.pravega.stream.Api;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.SegmentFutures;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.stream.SegmentId;
import com.emc.pravega.stream.Position;
import com.emc.pravega.stream.impl.PositionImpl;
import org.apache.commons.lang.NotImplementedException;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class ConsumerImpl implements Api.Consumer {

    private StreamMetadataStore streamStore;
    private HostControllerStore hostStore;

    private Map<Integer, List<Integer>> invertMap(Map<Integer, Integer> map) {
        Map<Integer, List<Integer>> inverse = new HashMap<>();
        map.entrySet().stream().forEach(
            x -> {
                if (inverse.containsKey(x.getValue())) {
                    inverse.get(x.getValue()).add(x.getKey());
                } else {
                    List<Integer> list = new ArrayList<>();
                    list.add(x.getKey());
                    inverse.put(x.getValue(), list);
                }
            }
        );
        return inverse;
    }

    public ConsumerImpl(StreamMetadataStore streamStore, HostControllerStore hostStore) {
        this.streamStore = streamStore;
        this.hostStore = hostStore;
    }

    @Override
    public CompletableFuture<List<Position>> getPositions(String stream, long timestamp, int n) {
        return CompletableFuture.supplyAsync(
            () ->
                {
                    SegmentFutures segmentFutures = streamStore.getActiveSegments(stream, timestamp);
                    int currentCount = segmentFutures.getCurrent().size();
                    int quotient = currentCount / n;
                    int remainder = currentCount % n;

                    Map<Integer, List<Integer>> inverse = invertMap(segmentFutures.getFutures());

                    int size = (quotient < 1) ? remainder : n;
                    List<Position> positions = new ArrayList<>(size);

                    int counter = 0;
                    for (int i = 0; i < size; i++) {
                        int j = (i < remainder) ? quotient + 1: quotient;
                        List<SegmentId> current = new ArrayList<>(j);
                        for (int k = 0; k < j; k++, counter++) {
                            Integer number = segmentFutures.getCurrent().get(counter);
                            SegmentId segmentId = new SegmentId(stream, stream+number, number, 0, "", 0);
                            current.add(segmentId);
                        }
                        Map<SegmentId, Long> currentSegments = new HashMap<>();
                        Map<SegmentId, Long> futureSegments = new HashMap<>();
                        current.forEach(
                            x ->
                                {
                                    // TODO fetch correct offset within the segment at specified timestamp by contacting pravega host
                                    currentSegments.put(x, 0L);
                                    int previous = x.getNumber();
                                    if (inverse.containsKey(previous)) {
                                        inverse.get(previous).stream().forEach(
                                            y ->
                                                {
                                                    SegmentId segmentId = new SegmentId(stream, stream+y, y, previous, "", 0);
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

    @Override
    public CompletableFuture<List<Position>> updatePositions(List<Position> positions) {
        throw new NotImplementedException();
    }
}
