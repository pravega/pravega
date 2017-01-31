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
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.FutureSegment;
import com.emc.pravega.controller.stream.api.v1.NodeUri;
import com.emc.pravega.controller.stream.api.v1.Position;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentRange;
import com.emc.pravega.controller.stream.api.v1.TxnId;
import com.emc.pravega.controller.stream.api.v1.TxnState;
import com.emc.pravega.controller.stream.api.v1.TxnStatus;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.PositionInternal;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Stream controller RPC server implementation.
 */
public class ControllerService {

    private final StreamMetadataStore streamStore;
    private final HostControllerStore hostStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;

    public ControllerService(final StreamMetadataStore streamStore,
                             final HostControllerStore hostStore,
                             final StreamMetadataTasks streamMetadataTasks,
                             final StreamTransactionMetadataTasks streamTransactionMetadataTasks) {
        this.streamStore = streamStore;
        this.hostStore = hostStore;
        this.streamMetadataTasks = streamMetadataTasks;
        this.streamTransactionMetadataTasks = streamTransactionMetadataTasks;
    }

    public CompletableFuture<CreateStreamStatus> createStream(final StreamConfiguration streamConfig, final long createTimestamp) {
        return streamMetadataTasks.createStream(streamConfig.getScope(), streamConfig.getName(), streamConfig, createTimestamp, Optional.empty());
    }

    public CompletableFuture<UpdateStreamStatus> alterStream(final StreamConfiguration streamConfig) {
        return streamMetadataTasks.alterStream(streamConfig.getScope(), streamConfig.getName(), streamConfig, Optional.empty());
    }

    public CompletableFuture<UpdateStreamStatus> sealStream(final String scope, final String stream) {
        return streamMetadataTasks.sealStream(scope, stream, Optional.empty());
    }

    public CompletableFuture<List<SegmentRange>> getCurrentSegments(final String scope, final String stream) {
        // fetch active segments from segment store
        return streamStore.getActiveSegments(scope, stream, null)
                .thenApply(activeSegments -> activeSegments
                                .stream()
                                .map(segment -> convert(scope, stream, segment))
                                .collect(Collectors.toList())
                );
    }

    public CompletableFuture<List<Position>> getPositions(final String scope, final String stream, final long timestamp, final int count) {
        // first fetch segments active at specified timestamp from the specified stream
        // divide current segments in segmentFutures into at most count positions
        return streamStore.getActiveSegments(scope, stream, timestamp, null)
                .thenApply(segmentFutures -> shard(scope, stream, segmentFutures, count));
    }

    public CompletableFuture<List<Position>> updatePositions(final String scope, final String stream, final List<Position> positions) {
        // TODO: handle npe with null exception return case

        List<PositionInternal> internalPositions = positions.stream().map(ModelHelper::encode).collect(Collectors.toList());
        // initialize completed segments set from those found in the list of input position objects
        Set<Integer> completedSegments = ModelHelper.getSegmentsFromPositions(internalPositions);

        Map<Integer, Long> segmentOffsets = new HashMap<>();

        // convert positions to segmentFutures, while updating completedSegments set and
        // storing segment offsets in segmentOffsets map
        List<SegmentFutures> segmentFutures = convertPositionsToSegmentFutures(internalPositions, segmentOffsets);

        // fetch updated SegmentFutures from stream metadata
        // and finally convert SegmentFutures back to position objects
        return streamStore.getNextSegments(scope, stream, completedSegments, segmentFutures, null)
                .thenApply(updatedSegmentFutures ->
                        convertSegmentFuturesToPositions(scope, stream, updatedSegmentFutures, segmentOffsets));
    }

    public CompletableFuture<ScaleResponse> scale(final String scope,
                                                  final String stream,
                                                  final List<Integer> sealedSegments,
                                                  final Map<Double, Double> newKeyRanges,
                                                  final long scaleTimestamp) {
        return streamMetadataTasks.scale(scope, stream, new ArrayList<>(sealedSegments), new ArrayList<>(ModelHelper.encode(newKeyRanges)), scaleTimestamp, Optional.empty());
    }

    public CompletableFuture<NodeUri> getURI(final SegmentId segment) throws TException {
        return CompletableFuture.completedFuture(
                SegmentHelper.getSegmentUri(segment.getScope(), segment.getStreamName(), segment.getNumber(), hostStore)
        );
    }

    private SegmentRange convert(final String scope,
                                 final String stream,
                                 final com.emc.pravega.controller.store.stream.Segment segment) {
        return new SegmentRange(
                new SegmentId(scope, stream, segment.getNumber()), segment.getKeyStart(), segment.getKeyEnd());
    }

    public CompletableFuture<Boolean> isSegmentValid(final String scope,
                                                     final String stream,
                                                     final int segmentNumber) throws TException {
        return streamStore.getActiveSegments(scope, stream, null)
                .thenApply(x -> x.stream().anyMatch(z -> z.getNumber() == segmentNumber));
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
    private List<Position> shard(final String scope, final String stream, final SegmentFutures segmentFutures, final int n) {
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
    private List<SegmentFutures> convertPositionsToSegmentFutures(final List<PositionInternal> positions,
                                                                  final Map<Integer, Long> segmentOffsets) {
        List<SegmentFutures> segmentFutures = new ArrayList<>(positions.size());

        // construct SegmentFutures for each position object.
        for (PositionInternal position : positions) {
            Map<Integer, Long> segmentOffsetMap = ModelHelper.toSegmentOffsetMap(position);
            segmentOffsets.putAll(segmentOffsetMap);
            Map<Integer, Integer> futures = ModelHelper.getFutureSegmentMap(position);
            segmentFutures.add(new SegmentFutures(new ArrayList<>(segmentOffsetMap.keySet()), futures));
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

    public CompletableFuture<TxnId> createTransaction(final String scope, final String stream) {
        // Note: We acquire an interprocess ephemeral lock before creating the transaction. The purpose of this is to ensure
        // that we mimic a priority queue per stream for incoming operation requests. Two kinds of operations contest for these locks -
        // createTxn and scale. Scale is given higher priority hence a write lock and
        return streamTransactionMetadataTasks.createTx(scope, stream, Optional.empty()).thenApply(ModelHelper::decode);
    }

    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream, final TxnId
            txnId) {
        return streamTransactionMetadataTasks.commitTx(scope, stream, ModelHelper.encode(txnId), Optional.empty())
                .handle((ok, ex) -> {
                    if (ex != null) {
                        // TODO: return appropriate failures to user
                        return TxnStatus.FAILURE;
                    } else {
                        return TxnStatus.SUCCESS;
                    }
                });
    }

    public CompletableFuture<TxnStatus> dropTransaction(final String scope, final String stream, final TxnId txnId) {
        return streamTransactionMetadataTasks.dropTx(scope, stream, ModelHelper.encode(txnId), Optional.empty())
                .handle((ok, ex) -> {
                    if (ex != null) {
                        // TODO: return appropriate failures to user
                        return TxnStatus.FAILURE;
                    } else {
                        return TxnStatus.SUCCESS;
                    }
                });
    }


    public CompletableFuture<TxnState> checkTransactionStatus(final String scope, final String stream, final TxnId
            txnId) {
        return streamStore.transactionStatus(scope, stream, ModelHelper.encode(txnId), null)
                .thenApply(ModelHelper::decode);
    }
}
