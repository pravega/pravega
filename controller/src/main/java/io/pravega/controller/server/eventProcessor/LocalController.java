/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.stream.PingFailedException;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.Transaction;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerFailureException;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.client.stream.impl.SegmentWithRange;
import io.pravega.client.stream.impl.StreamSegments;
import io.pravega.client.stream.impl.StreamSegmentsWithPredecessors;
import io.pravega.client.stream.impl.TxnSegments;
import io.pravega.common.concurrent.FutureHelpers;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import io.pravega.shared.protocol.netty.PravegaNodeUri;
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
    public CompletableFuture<Boolean> createScope(final String scopeName) {
        return this.controller.createScope(scopeName).thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                throw new ControllerFailureException("Failed to create scope: " + scopeName);
            case INVALID_SCOPE_NAME:
                throw new IllegalArgumentException("Illegal scope name: " + scopeName);
            case SCOPE_EXISTS:
                return false;
            case SUCCESS:
                return true;
            default:
                throw new ControllerFailureException("Unknown return status creating scope " + scopeName + " "
                        + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> deleteScope(String scopeName) {
        return this.controller.deleteScope(scopeName).thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                throw new ControllerFailureException("Failed to delete scope: " + scopeName);
            case SCOPE_NOT_EMPTY:
                throw new IllegalStateException("Scope "+ scopeName+ " is not empty.");
            case SCOPE_NOT_FOUND:
                return false;
            case SUCCESS:
                return true;
            default:
                throw new ControllerFailureException("Unknown return status deleting scope " + scopeName
                                                     + " " + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> createStream(final StreamConfiguration streamConfig) {
        return this.controller.createStream(streamConfig, System.currentTimeMillis()).thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                throw new ControllerFailureException("Failed to createing stream: " + streamConfig);
            case INVALID_STREAM_NAME:
                throw new IllegalArgumentException("Illegal stream name: " + streamConfig);
            case SCOPE_NOT_FOUND:
                throw new IllegalArgumentException("Scope does not exist: " + streamConfig);
            case STREAM_EXISTS:
                return false;
            case SUCCESS:
                return true;
            default:
                throw new ControllerFailureException("Unknown return status creating stream " + streamConfig
                                                     + " " + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> alterStream(final StreamConfiguration streamConfig) {
        return this.controller.alterStream(streamConfig).thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                throw new ControllerFailureException("Failed to altering stream: " + streamConfig);
            case SCOPE_NOT_FOUND:
                throw new IllegalArgumentException("Scope does not exist: " + streamConfig);
            case STREAM_NOT_FOUND:
                throw new IllegalArgumentException("Stream does not exist: " + streamConfig);
            case SUCCESS:
                return true;
            default:
                throw new ControllerFailureException("Unknown return status altering stream " + streamConfig
                                                     + " " + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> sealStream(String scope, String streamName) {
        return this.controller.sealStream(scope, streamName).thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                throw new ControllerFailureException("Failed to seal stream: " + streamName);
            case SCOPE_NOT_FOUND:
                throw new IllegalArgumentException("Scope does not exist: " + scope);
            case STREAM_NOT_FOUND:
                throw new IllegalArgumentException("Stream does not exist: " + streamName);
            case SUCCESS:
                return true;
            default:
                throw new ControllerFailureException("Unknown return status scealing stream " + streamName
                                                     + " " + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> deleteStream(final String scope, final String streamName) {
        return this.controller.deleteStream(scope, streamName).thenApply(x -> {
            switch (x.getStatus()) {
            case FAILURE:
                throw new ControllerFailureException("Failed to delete stream: " + streamName);
            case STREAM_NOT_FOUND:
                return false;
            case STREAM_NOT_SEALED:
                throw new IllegalArgumentException("Stream is not sealed: " + streamName);
            case SUCCESS:
                return true;
            default:
                throw new ControllerFailureException("Unknown return status deleting stream " + streamName + " "
                        + x.getStatus());
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> scaleStream(final Stream stream,
                                                  final List<Integer> sealedSegments,
                                                  final Map<Double, Double> newKeyRanges) {
        return this.controller.scale(stream.getScope(),
                                     stream.getStreamName(),
                                     sealedSegments,
                                     newKeyRanges,
                                     System.currentTimeMillis())
                .thenApply(x -> {
                    switch (x.getStatus()) {
                    case FAILURE:
                        throw new ControllerFailureException("Failed to scale stream: " + stream);
                    case PRECONDITION_FAILED:
                        return false;
                    case SUCCESS:
                        return true;
                    case TXN_CONFLICT:
                        throw new ControllerFailureException("Controller failed to properly abort transactions on stream: "
                                + stream);
                    default:
                        throw new ControllerFailureException("Unknown return status scaling stream "
                                + stream + " " + x.getStatus());
                    }
                });
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(final String scope, final String streamName) {
        return controller.getCurrentSegments(scope, streamName)
                .thenApply(this::getStreamSegments);
    }

    private StreamSegments getStreamSegments(List<SegmentRange> ranges) {
        NavigableMap<Double, Segment> rangeMap = new TreeMap<>();
        for (SegmentRange r : ranges) {
            rangeMap.put(r.getMaxKey(), ModelHelper.encode(r.getSegmentId()));
        }
        return new StreamSegments(rangeMap);
    }

    @Override
    public CompletableFuture<TxnSegments> createTransaction(Stream stream, long lease, final long maxExecutionTime,
                                                     final long scaleGracePeriod) {
        return controller
                .createTransaction(stream.getScope(), stream.getStreamName(), lease, maxExecutionTime, scaleGracePeriod)
                .thenApply(pair -> {
                    return new TxnSegments(getStreamSegments(pair.getRight()), pair.getKey());
                });
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
    public CompletableFuture<StreamSegmentsWithPredecessors> getSuccessors(Segment segment) {
        return controller.getSegmentsImmediatelyFollowing(ModelHelper.decode(segment))
                .thenApply(x -> {
                    Map<SegmentWithRange, List<Integer>> map = new HashMap<>();
                    x.forEach((segmentId, list) -> map.put(ModelHelper.encode(segmentId), list));
                    return new StreamSegmentsWithPredecessors(map);
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
