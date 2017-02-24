/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.server.eventProcessor;

import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.server.rpc.v1.ControllerService;
import com.emc.pravega.controller.stream.api.v1.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.v1.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.v1.ScaleResponse;
import com.emc.pravega.controller.stream.api.v1.SegmentId;
import com.emc.pravega.controller.stream.api.v1.SegmentRange;
import com.emc.pravega.controller.stream.api.v1.UpdateStreamStatus;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.impl.Controller;
import com.emc.pravega.stream.impl.ModelHelper;
import com.emc.pravega.stream.impl.PositionInternal;
import com.emc.pravega.stream.impl.StreamSegments;
import org.apache.thrift.TException;

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
    public CompletableFuture<CreateScopeStatus> createScope(String scopeName) {
        return this.controller.createScope(scopeName);
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
    public CompletableFuture<UUID> createTransaction(Stream stream, long timeout) {
        return controller.createTransaction(stream.getScope(), stream.getStreamName())
                .thenApply(ModelHelper::encode);
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
                .thenApply(status -> ModelHelper.encode(status, stream + " " + txnId));
    }

    @Override
    public CompletableFuture<List<PositionInternal>> getPositions(Stream stream, long timestamp, int count) {
        return controller.getPositions(stream.getScope(), stream.getStreamName(), timestamp, count)
                .thenApply(result -> result.stream().map(ModelHelper::encode).collect(Collectors.toList()));
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
        try {
            return controller.getURI(new SegmentId(segment.getScope(), segment.getStreamName(),
                    segment.getSegmentNumber())).thenApply(ModelHelper::encode);
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }
}