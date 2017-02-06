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
package com.emc.pravega.controller.server;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.controller.store.host.HostControllerStore;
import com.emc.pravega.controller.store.stream.Segment;
import com.emc.pravega.controller.store.stream.SegmentFutures;
import com.emc.pravega.controller.store.stream.StreamMetadataStore;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.FutureSegment;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.Position;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import com.emc.pravega.controller.task.Stream.StreamMetadataTasks;
import com.emc.pravega.controller.task.Stream.StreamTransactionMetadataTasks;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.PositionInternal;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimaps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Stream controller RPC server implementation.
 */
@Getter
@AllArgsConstructor
public class ControllerService {

    private final StreamMetadataStore streamStore;
    private final HostControllerStore hostStore;
    private final StreamMetadataTasks streamMetadataTasks;
    private final StreamTransactionMetadataTasks streamTransactionMetadataTasks;

    public CompletableFuture<CreateStreamStatus> createStream(final StreamConfiguration streamConfig,
            final long createTimestamp) {
        Preconditions.checkNotNull(streamConfig, "streamConfig");
        Preconditions.checkArgument(createTimestamp >= 0);
        return streamMetadataTasks.createStream(streamConfig.getScope(),
                                                streamConfig.getName(),
                                                streamConfig,
                                                createTimestamp)
                .thenApply(status -> CreateStreamStatus.newBuilder().setStatus(status).build());
    }

    public CompletableFuture<UpdateStreamStatus> alterStream(final StreamConfiguration streamConfig) {
        Preconditions.checkNotNull(streamConfig, "streamConfig");
        return streamMetadataTasks.alterStream(streamConfig.getScope(), streamConfig.getName(), streamConfig)
                .thenApply(status -> UpdateStreamStatus.newBuilder().setStatus(status).build());
    }

    public CompletableFuture<UpdateStreamStatus> sealStream(final String scope, final String stream) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return streamMetadataTasks.sealStream(scope, stream)
                .thenApply(status -> UpdateStreamStatus.newBuilder().setStatus(status).build());
    }

    public CompletableFuture<List<SegmentRange>> getCurrentSegments(final String scope, final String stream) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");

        // Fetch active segments from segment store.
        return streamStore.getActiveSegments(stream)
                .thenApply(activeSegments -> activeSegments
                                .stream()
                                .map(segment -> convert(scope, stream, segment))
                                .collect(Collectors.toList())
                );
    }

    public CompletableFuture<List<Position>> getPositions(final String scope, final String stream, final long timestamp,
            final int count) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");

        // First fetch segments active at specified timestamp from the specified stream.
        // Divide current segments in segmentFutures into at most count positions.
        return streamStore.getActiveSegments(stream, timestamp)
                .thenApply(segmentFutures -> shard(scope, stream, segmentFutures, count));
    }

    public CompletableFuture<List<Position>> updatePositions(final String scope, final String stream,
            final List<Position> positions) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(positions, "positions");

        // TODO: handle npe with null exception return case.
        List<PositionInternal> internalPositions =
                positions.stream().map(ModelHelper::encode).collect(Collectors.toList());

        // Initialize completed segments set from those found in the list of input position objects.
        Set<Integer> completedSegments = ModelHelper.getSegmentsFromPositions(internalPositions);

        Map<Integer, Long> segmentOffsets = new HashMap<>();

        // Convert positions to segmentFutures, while updating completedSegments set and
        // storing segment offsets in segmentOffsets map.
        List<SegmentFutures> segmentFutures = convertPositionsToSegmentFutures(internalPositions, segmentOffsets);

        // Fetch updated SegmentFutures from stream metadata
        // and finally convert SegmentFutures back to position objects.
        return streamStore.getNextSegments(stream, completedSegments, segmentFutures)
                .thenApply(updatedSegmentFutures ->
                        convertSegmentFuturesToPositions(scope, stream, updatedSegmentFutures, segmentOffsets));
    }

    public CompletableFuture<ScaleResponse> scale(final String scope,
                                                  final String stream,
                                                  final List<Integer> sealedSegments,
                                                  final Map<Double, Double> newKeyRanges,
                                                  final long scaleTimestamp) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(sealedSegments, "sealedSegments");
        Preconditions.checkNotNull(newKeyRanges, "newKeyRanges");

        return streamMetadataTasks.scale(scope,
                                         stream,
                                         new ArrayList<>(sealedSegments),
                                         new ArrayList<>(ModelHelper.encode(newKeyRanges)),
                                         scaleTimestamp);
    }

    public CompletableFuture<NodeUri> getURI(final SegmentId segment) {
        Preconditions.checkNotNull(segment, "segment");
        return CompletableFuture.completedFuture(
                SegmentHelper.getSegmentUri(segment.getStreamInfo().getScope(), segment.getStreamInfo().getStream(),
                                            segment.getSegmentNumber(), hostStore)
        );
    }

    private SegmentRange convert(final String scope,
                                 final String stream,
                                 final Segment segment) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(segment, "segment");
        return ModelHelper.createSegmentRange(
                scope, stream, segment.getNumber(), segment.getKeyStart(), segment.getKeyEnd());
    }

    public CompletableFuture<Boolean> isSegmentValid(final String scope,
                                                     final String stream,
                                                     final int segmentNumber) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return streamStore.getActiveSegments(stream)
                .thenApply(x -> x.stream().anyMatch(z -> z.getNumber() == segmentNumber));
    }

    public CompletableFuture<TxnId> createTransaction(final String scope, final String stream) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        return streamTransactionMetadataTasks.createTx(scope, stream).thenApply(ModelHelper::decode);
    }

    public CompletableFuture<TxnStatus> commitTransaction(final String scope, final String stream, final TxnId txnId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txnId, "txnId");
        return streamTransactionMetadataTasks.commitTx(scope, stream, ModelHelper.encode(txnId))
                .handle((ok, ex) -> {
                    if (ex != null) {
                        // TODO: return appropriate failures to user.
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.FAILURE).build();
                    } else {
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build();
                    }
                });
    }

    public CompletableFuture<TxnStatus> dropTransaction(final String scope, final String stream, final TxnId txnId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txnId, "txnId");
        return streamTransactionMetadataTasks.dropTx(scope, stream, ModelHelper.encode(txnId))
                .handle((ok, ex) -> {
                    if (ex != null) {
                        // TODO: return appropriate failures to user.
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.FAILURE).build();
                    } else {
                        return TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build();
                    }
                });
    }

    public CompletableFuture<TxnState> checkTransactionState(final String scope, final String stream,
            final TxnId txnId) {
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");
        Preconditions.checkNotNull(txnId, "txnId");
        return streamStore.transactionStatus(scope, stream, ModelHelper.encode(txnId))
                .thenApply(res -> TxnState.newBuilder().setState(ModelHelper.decode(res)).build());
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
    private List<Position> shard(final String scope, final String stream, final SegmentFutures segmentFutures,
            final int n) {
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
                SegmentId segmentId = ModelHelper.createSegmentId(scope, stream, number);
                current.add(segmentId);
            }

            // Compute the current and future segments set for position i.
            Map<SegmentId, Long> currentSegments = new HashMap<>();
            Map<FutureSegment, Long> futureSegments = new HashMap<>();
            current.stream().forEach(
                    x -> {
                        // TODO fetch correct offset within the segment at specified timestamp by contacting pravega
                        // host put it in the currentSegments.
                        currentSegments.put(x, 0L);

                        // Update futures with all segments in segmentFutures.getFutures having x.number as the
                        // predecessor these segments can be found from the inverted segmentFutures.getFutures.
                        int previous = x.getSegmentNumber();
                        if (inverse.containsKey(previous)) {
                            inverse.get(previous).stream().forEach(
                                    y -> {
                                        SegmentId newSegment = ModelHelper.createSegmentId(scope, stream, y);
                                        SegmentId oldSegment = ModelHelper.createSegmentId(scope, stream, previous);
                                        futureSegments.put(FutureSegment.newBuilder()
                                                                   .setFutureSegment(newSegment)
                                                                   .setPrecedingSegment(oldSegment)
                                                                   .build(),
                                                           0L);
                                    }
                            );
                        }
                    }
            );

            // Create a new position object with current and futures segments thus computed.
            Position position = Position.newBuilder()
                    .addAllOwnedSegments(currentSegments.entrySet().stream().
                            map(val -> Position.OwnedSegmentEntry.newBuilder()
                                    .setSegmentId(val.getKey())
                                    .setValue(val.getValue())
                                    .build()).collect(Collectors.toList()))
                    .addAllFutureOwnedSegments(futureSegments.entrySet().stream().
                            map(val -> Position.FutureOwnedSegmentsEntry.newBuilder()
                                    .setFutureSegment(val.getKey())
                                    .setValue(val.getValue())
                                    .build()).collect(Collectors.toList()))
                    .build();
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

        // Construct SegmentFutures for each position object.
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
                            current -> currentSegments.put(ModelHelper.createSegmentId(scope, stream, current),
                                                           segmentOffsets.get(current))
                    );
                    future.getFutures().entrySet().stream().forEach(
                            y -> futureSegments.put(
                                    FutureSegment.newBuilder()
                                            .setFutureSegment(ModelHelper.createSegmentId(scope, stream, y.getKey()))
                                            .setPrecedingSegment(
                                                    ModelHelper.createSegmentId(scope, stream, y.getValue()))
                                            .build(),
                                    0L)
                    );
                    Position position = Position.newBuilder()
                            .addAllOwnedSegments(currentSegments.entrySet().stream().
                                    map(val -> Position.OwnedSegmentEntry.newBuilder()
                                            .setSegmentId(val.getKey())
                                            .setValue(val.getValue())
                                            .build()).collect(Collectors.toList()))
                            .addAllFutureOwnedSegments(futureSegments.entrySet().stream().
                                    map(val -> Position.FutureOwnedSegmentsEntry.newBuilder()
                                            .setFutureSegment(val.getKey())
                                            .setValue(val.getValue())
                                            .build()).collect(Collectors.toList()))
                            .build();
                    resultPositions.add(position);
                }
        );
        return resultPositions;
    }
}
