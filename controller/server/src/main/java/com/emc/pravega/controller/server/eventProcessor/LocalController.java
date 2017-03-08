/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.server.ControllerService;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import com.emc.pravega.stream.PingFailedException;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.StreamSegments;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class LocalController implements Controller {

    private ControllerService controller;

    public LocalController(ControllerService controller) {
        this.controller = controller;
    }

    @Override
    public CompletableFuture<CreateScopeStatus> createScope(final String scopeName) {
        return this.controller.createScope(scopeName);
    }

    @Override
    public CompletableFuture<DeleteScopeStatus> deleteScope(String scopeName) {
        return this.controller.deleteScope(scopeName);
    }

    @Override
    public CompletableFuture<CreateStreamStatus> createStream(final StreamConfiguration streamConfig) {
        return this.controller.createStream(streamConfig, System.currentTimeMillis());
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> alterStream(final StreamConfiguration streamConfig) {
        return this.controller.alterStream(streamConfig);
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> sealStream(String scope, String streamName) {
        return this.controller.sealStream(scope, streamName);
    }

    @Override
    public CompletableFuture<ScaleResponse> scaleStream(final Stream stream,
                                                        final List<Integer> sealedSegments,
                                                        final Map<Double, Double> newKeyRanges) {
        return this.controller.scale(stream.getScope(),
                stream.getStreamName(),
                sealedSegments,
                newKeyRanges,
                System.currentTimeMillis());
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(final String scope, final String streamName) {
        return controller.getCurrentSegments(scope, streamName)
                .thenApply((List<SegmentRange> ranges) -> {
                    NavigableMap<Double, Segment> rangeMap = new TreeMap<>();
                    for (SegmentRange r : ranges) {
                        rangeMap.put(r.getMaxKey(), ModelHelper.encode(r.getSegmentId()));
                    }
                    return rangeMap;
                })
                .thenApply(StreamSegments::new);
    }

    @Override
    public CompletableFuture<UUID> createTransaction(Stream stream, long lease, final long maxExecutionTime,
                                                     final long scaleGracePeriod) {
        return controller
                .createTransaction(stream.getScope(), stream.getStreamName(), lease, maxExecutionTime, scaleGracePeriod)
                .thenApply(ModelHelper::encode);
    }

    @Override
    public CompletableFuture<Void> pingTransaction(Stream stream, UUID txId, long lease) {
        return FutureHelpers.toVoidExpecting(
                controller.pingTransaction(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txId), lease),
                PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.OK).build(),
                PingFailedException::new);
    }

    @Override
    public CompletableFuture<Void> commitTransaction(Stream stream, UUID txnId) {
        return controller
                .commitTransaction(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txnId))
                .thenApply(x -> null);
    }

    @Override
    public CompletableFuture<Void> abortTransaction(Stream stream, UUID txId) {
        return controller
                .abortTransaction(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txId))
                .thenApply(x -> null);
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(Stream stream, UUID txnId) {
        return controller.checkTransactionStatus(stream.getScope(), stream.getStreamName(), ModelHelper.decode(txnId))
                .thenApply(status -> ModelHelper.encode(status.getState(), stream + " " + txnId));
    }

    @Override
    public CompletableFuture<Map<Segment, Long>> getSegmentsAtTime(Stream stream, long timestamp) {
        return controller.getSegmentsAtTime(stream.getScope(), stream.getStreamName(), timestamp).thenApply(segments -> {
            return segments.entrySet()
                           .stream()
                           .collect(Collectors.toMap(entry -> ModelHelper.encode(entry.getKey()),
                                                     entry -> entry.getValue()));
        });
    }

    @Override
    public CompletableFuture<Map<Segment, List<Integer>>> getSuccessors(Segment segment) {
        return controller.getSegmentsImmediatlyFollowing(ModelHelper.decode(segment))
                .thenApply(x -> {
                    Map<Segment, List<Integer>> map = new HashMap<>();
                    x.forEach((segmentId, list) -> map.put(ModelHelper.encode(segmentId), list));
                    return map;
                });
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(String qualifiedSegmentName) {
        Segment segment = Segment.fromScopedName(qualifiedSegmentName);
            return controller.getURI(ModelHelper.createSegmentId(segment.getScope(), segment.getStreamName(),
                    segment.getSegmentNumber())).thenApply(ModelHelper::encode);
    }

    @Override
    public CompletableFuture<Boolean> isSegmentOpen(Segment segment) {
        return controller.isSegmentValid(segment.getScope(), segment.getStreamName(), segment.getSegmentNumber());
    }
}
