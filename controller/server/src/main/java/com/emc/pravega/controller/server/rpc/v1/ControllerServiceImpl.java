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
package com.emc.pravega.controller.server.rpc.v1;

import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.SegmentFutures;
import com.emc.pravega.controller.store.stream.SegmentNotFoundException;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.FutureSegment;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.stream.api.v1.Position;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentRange;
import com.emc.pravega.controller.stream.api.v1.Status;
import com.emc.pravega.controller.stream.api.v1.TxId;
import com.emc.pravega.stream.PositionInternal;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.model.ModelHelper;
import com.emc.pravega.stream.impl.netty.ConnectionFactoryImpl;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Stream controller RPC server implementation
 */
public class ControllerServiceImpl {

    private final StreamMetadataStore streamStore;
    private final HostControllerStore hostStore;
    private ConnectionFactoryImpl connectionFactory;

    public ControllerServiceImpl(StreamMetadataStore streamStore, HostControllerStore hostStore) {
        this.streamStore = streamStore;
        this.hostStore = hostStore;
        this.connectionFactory = new ConnectionFactoryImpl(false);
    }

    public CompletableFuture<Status> createStream(StreamConfiguration streamConfig) {
        String stream = streamConfig.getName();

        return streamStore.createStream(stream, streamConfig)
                .thenApply(result -> {
                    if (result) {
                        streamStore.getActiveSegments(stream)
                                .thenApply(activeSegments -> {
                                    activeSegments
                                            .stream()
                                            .parallel()
                                            .forEach(segment -> notifyNewSegment(streamConfig.getScope(), stream, segment.getNumber()));
                                    return null;
                                });
                        return Status.SUCCESS;
                    } else {
                        return Status.FAILURE;
                    }
                });
    }

    public CompletableFuture<Status> alterStream(StreamConfiguration streamConfig) {
        throw new NotImplementedException();
    }

    public CompletableFuture<List<SegmentRange>> getCurrentSegments(String scope, String stream) {
        // fetch active segments from segment store
        return streamStore.getActiveSegments(stream)
                .thenApply(activeSegments -> activeSegments
                                .stream()
                                .map(segment ->
                                                new SegmentRange(
                                                        new SegmentId(scope, stream, segment.getNumber()),
                                                        segment.getKeyStart(),
                                                        segment.getKeyEnd())
                                )
                                .collect(Collectors.toList())
                );
    }

    public CompletableFuture<List<Position>> getPositions(String scope, String stream, long timestamp, int count) {
        // first fetch segments active at specified timestamp from the specified stream
        // divide current segments in segmentFutures into at most count positions
        return streamStore.getActiveSegments(stream, timestamp)
                .thenApply(segmentFutures -> shard(scope, stream, segmentFutures, count));
    }

    public CompletableFuture<List<Position>> updatePositions(String scope, String stream, List<Position> positions) {
        // TODO: handle npe with null exception return case
        List<PositionInternal> internalPositions = positions.stream().map(ModelHelper::encode).collect(Collectors.toList());
        // initialize completed segments set from those found in the list of input position objects
        Set<Integer> completedSegments = internalPositions.stream().flatMap(position ->
                        position.getCompletedSegments().stream().map(Segment::getSegmentNumber)
        ).collect(Collectors.toSet());

        Map<Integer, Long> segmentOffsets = new HashMap<>();

        // convert positions to segmentFutures, while updating completedSegments set and
        // storing segment offsets in segmentOffsets map
        List<SegmentFutures> segmentFutures = convertPositionsToSegmentFutures(internalPositions, segmentOffsets);

        // fetch updated SegmentFutures from stream metadata
        // and finally convert SegmentFutures back to position objects
        return streamStore.getNextSegments(stream, completedSegments, segmentFutures)
                .thenApply(updatedSegmentFutures ->
                        convertSegmentFuturesToPositions(scope, stream, updatedSegmentFutures, segmentOffsets));
    }

    public CompletableFuture<NodeUri> getURI(SegmentId segment) throws TException {
        return CompletableFuture.completedFuture(
                SegmentHelper.getSegmentUri(segment.getScope(), segment.getStreamName(), segment.getNumber(), hostStore)
        );
    }

    public CompletableFuture<Boolean> isSegmentValid(String scope, String stream, int segmentNumber, String caller) throws TException {
        return streamStore.getSegment(stream, segmentNumber).handle((ok, ex) -> {
            if (ex != null) {
                if(ex instanceof SegmentNotFoundException)
                    return false;
                else throw new RuntimeException(ex);
            } else
                return SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostStore).getEndpoint().equals(caller);
        });
    }

    private void notifyNewSegment(String scope, String stream, int segmentNumber) {
        NodeUri uri = SegmentHelper.getSegmentUri(scope, stream, segmentNumber, hostStore);

        // async call, dont wait for its completion or success. Host will contact controller if it does not know
        // about some segment even if this call fails
        CompletableFuture.runAsync(() -> SegmentHelper.createSegment(scope, stream, segmentNumber, ModelHelper.encode(uri), connectionFactory));
    }

    /**
     * This method divides the current segments from the segmentFutures into at most n positions. It appropriately
     * distributes the future segments in segmentFutures among the shards. E.g., if n=5, and segmentFutures contains
     * a) 3 current segments, then 3 positions will be created each having one current segment
     * b) 6 current segments, then 5 positions will be created 1st position containing #1, #2 current segments
     * and remaining positions having 1 current segment each
     *
     * @param stream         input stream
     * @param segmentFutures input segmentFutures
     * @param n              number of shards
     * @return the list of position objects
     */
    private List<Position> shard(String scope, String stream, SegmentFutures segmentFutures, int n) {
        // divide the active segments equally into at most n partition
        int currentCount = segmentFutures.getCurrent().size();
        int quotient = currentCount / n;
        int remainder = currentCount % n;
        // if quotient < 1 then remainder number of positions shall be created, other wise n positions shall be created
        int size = (quotient < 1) ? remainder : n;
        List<Position> positions = new ArrayList<>(size);

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
                SegmentId segmentId = new SegmentId(scope, stream, number);
                current.add(segmentId);
            }

            // Compute the current and future segments set for position i
            Map<SegmentId, Long> currentSegments = new HashMap<>();
            Map<FutureSegment, Long> futureSegments = new HashMap<>();
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
                                        SegmentId newSegment = new SegmentId(scope, stream, y);
                                        SegmentId oldSegment = new SegmentId(scope, stream, previous);
                                        futureSegments.put(new FutureSegment(newSegment, oldSegment), 0L);
                                    }
                            );
                        }
                    }
            );
            // create a new position object with current and futures segments thus computed
            Position position = new Position(currentSegments, futureSegments);
            positions.add(position);
        }
        return positions;
    }

    /**
     * This method converts list of positions into list of segmentFutures.
     * While doing so it updates the completedSegments set and stores segment offsets in a map.
     *
     * @param positions      input list of positions
     * @param segmentOffsets map of segment number of its offset that shall be populated in this method
     * @return the list of segmentFutures objects
     */
    private List<SegmentFutures> convertPositionsToSegmentFutures(List<PositionInternal> positions, Map<Integer, Long> segmentOffsets) {
        List<SegmentFutures> segmentFutures = new ArrayList<>(positions.size());

        // construct SegmentFutures for each position object.
        for (PositionInternal position : positions) {
            List<Integer> current = new ArrayList<>(position.getOwnedSegments().size());
            Map<Integer, Integer> futures = new HashMap<>();
            position.getOwnedSegmentsWithOffsets().entrySet().stream().forEach(
                    x -> {
                        int number = x.getKey().getSegmentNumber();
                        current.add(number);
                        segmentOffsets.put(number, x.getValue());
                    }
            );
            position.getFutureOwnedSegments().stream().forEach(x -> futures.put(x.getSegmentNumber(), x.getPrecedingNumber()));
            segmentFutures.add(new SegmentFutures(current, futures));
        }
        return segmentFutures;
    }

    private List<Position> convertSegmentFuturesToPositions(String scope, String stream, List<SegmentFutures> segmentFutures, Map<Integer, Long> segmentOffsets) {
        List<Position> resultPositions = new ArrayList<>(segmentFutures.size());
        segmentFutures.stream().forEach(
                future -> {
                    Map<SegmentId, Long> currentSegments = new HashMap<>();
                    Map<FutureSegment, Long> futureSegments = new HashMap<>();
                    future.getCurrent().stream().forEach(
                            current -> currentSegments.put(new SegmentId(scope, stream, current), segmentOffsets.get(current))
                    );
                    future.getFutures().entrySet().stream().forEach(
                            y -> futureSegments.put(new FutureSegment(new SegmentId(scope, stream, y.getKey()), new SegmentId(scope, stream, y.getValue())), 0L)
                    );
                    resultPositions.add(new Position(currentSegments, futureSegments));
                }
        );
        return resultPositions;
    }

    public CompletableFuture<TxId> createTransaction(String scope, String stream) {
        return null;
    }

    public CompletableFuture<Status> commitTransaction(String scope, String stream, TxId txid) {
        return null;
    }

    public CompletableFuture<Status> dropTransaction(String scope, String stream, TxId txid) {
        return null;
    }

    public CompletableFuture<Status> checkTransactionStatus(String scope, String stream, TxId txid) {
        return null;
    }
}
