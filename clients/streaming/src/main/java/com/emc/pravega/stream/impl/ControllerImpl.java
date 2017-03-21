/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.common.Exceptions;
import com.emc.pravega.common.LoggerHelpers;
import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.common.netty.PravegaNodeUri;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentRange;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnId;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import com.emc.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc;
import com.emc.pravega.stream.PingFailedException;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.StreamConfiguration;
import com.emc.pravega.stream.Transaction;
import com.emc.pravega.stream.TxnFailedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * RPC based client implementation of Stream Controller V1 API.
 */
@Slf4j
public class ControllerImpl implements Controller {

    // The gRPC client for the Controller Service.
    private final ControllerServiceGrpc.ControllerServiceStub client;

    /**
     * Creates a new instance of the Controller client class.
     *
     * @param host The controller rpc host name.
     * @param port The controller rpc port number.
     */
    public ControllerImpl(final String host, final int port) {
        this(ManagedChannelBuilder.forAddress(host, port).usePlaintext(true));
        log.info("Controller client connecting to server at {}:{}", host, port);
    }

    /**
     * Creates a new instance of the Controller client class.
     *
     * @param channelBuilder The channel builder to connect to the service instance.
     */
    @VisibleForTesting
    public ControllerImpl(ManagedChannelBuilder channelBuilder) {
        Preconditions.checkNotNull(channelBuilder, "channelBuilder");

        // Create Async RPC client.
        client = ControllerServiceGrpc.newStub(channelBuilder.build());
    }

    @Override
    public CompletableFuture<CreateScopeStatus> createScope(final String scopeName) {
        long traceId = LoggerHelpers.traceEnter(log, "createScope", scopeName);

        RPCAsyncCallback<CreateScopeStatus> callback = new RPCAsyncCallback<>();
        client.createScope(ScopeInfo.newBuilder().setScope(scopeName).build(), callback);
        return callback.getFuture()
                .thenApply(x -> {
                    LoggerHelpers.traceLeave(log, "createScope", traceId);
                    return x;
                });
    }

    @Override
    public CompletableFuture<DeleteScopeStatus> deleteScope(String scopeName) {
        long traceId = LoggerHelpers.traceEnter(log, "deleteScope", scopeName);
        log.trace("Invoke AdminService.Client.deleteScope() with name: {}", scopeName);

        RPCAsyncCallback<DeleteScopeStatus> callback = new RPCAsyncCallback<>();
        client.deleteScope(ScopeInfo.newBuilder().setScope(scopeName).build(), callback);
        return callback.getFuture()
                .thenApply(x -> {
                    LoggerHelpers.traceLeave(log, "deleteScope", traceId);
                    return x;
                });
    }

    @Override
    public CompletableFuture<CreateStreamStatus> createStream(final StreamConfiguration streamConfig) {
        long traceId = LoggerHelpers.traceEnter(log, "createStream", streamConfig);
        Preconditions.checkNotNull(streamConfig, "streamConfig");

        RPCAsyncCallback<CreateStreamStatus> callback = new RPCAsyncCallback<>();
        client.createStream(ModelHelper.decode(streamConfig), callback);
        return callback.getFuture()
                .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "createStream", traceId));
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> alterStream(final StreamConfiguration streamConfig) {
        long traceId = LoggerHelpers.traceEnter(log, "alterStream", streamConfig);
        Preconditions.checkNotNull(streamConfig, "streamConfig");

        RPCAsyncCallback<UpdateStreamStatus> callback = new RPCAsyncCallback<>();
        client.alterStream(ModelHelper.decode(streamConfig), callback);
        return callback.getFuture()
                .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "alterStream", traceId));
    }

    @Override
    public CompletableFuture<ScaleResponse> scaleStream(final Stream stream, final List<Integer> sealedSegments,
            final Map<Double, Double> newKeyRanges) {
        long traceId = LoggerHelpers.traceEnter(log, "scaleStream", stream);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(sealedSegments, "sealedSegments");
        Preconditions.checkNotNull(newKeyRanges, "newKeyRanges");

        RPCAsyncCallback<ScaleResponse> callback = new RPCAsyncCallback<>();
        client.scale(ScaleRequest.newBuilder()
                             .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(), stream.getStreamName()))
                             .addAllSealedSegments(sealedSegments)
                             .addAllNewKeyRanges(newKeyRanges.entrySet().stream()
                                                         .map(x -> ScaleRequest.KeyRangeEntry.newBuilder()
                                                                 .setStart(x.getKey()).setEnd(x.getValue()).build())
                                                         .collect(Collectors.toList()))
                             .setScaleTimestamp(System.currentTimeMillis())
                             .build(),
                     callback);
        return callback.getFuture()
                .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "scaleStream", traceId));
    }

    @Override
    public CompletableFuture<UpdateStreamStatus> sealStream(final String scope, final String streamName) {
        long traceId = LoggerHelpers.traceEnter(log, "sealStream", scope, streamName);
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");

        RPCAsyncCallback<UpdateStreamStatus> callback = new RPCAsyncCallback<>();
        client.sealStream(ModelHelper.createStreamInfo(scope, streamName), callback);
        return callback.getFuture()
                .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "sealStream", traceId));
    }

    @Override
    public CompletableFuture<DeleteStreamStatus> deleteStream(final String scope, final String streamName) {
        long traceId = LoggerHelpers.traceEnter(log, "deleteStream", scope, streamName);
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");

        RPCAsyncCallback<DeleteStreamStatus> callback = new RPCAsyncCallback<>();
        client.deleteStream(ModelHelper.createStreamInfo(scope, streamName), callback);
        return callback.getFuture()
                .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "deleteStream", traceId));
    }

    @Override
    public CompletableFuture<Map<Segment, Long>> getSegmentsAtTime(final Stream stream, final long timestamp) {
        Preconditions.checkNotNull(stream, "stream");

        log.trace("Invoke ConsumerService.Client.getSegmentsAtTime() for stream: {}, timestamp: {}", stream, timestamp);
        RPCAsyncCallback<SegmentsAtTime> callback = new RPCAsyncCallback<>();
        StreamInfo streamInfo = ModelHelper.createStreamInfo(stream.getScope(), stream.getStreamName());
        GetSegmentsRequest request = GetSegmentsRequest.newBuilder()
                                                       .setStreamInfo(streamInfo)
                                                       .setTimestamp(timestamp)
                                                       .build();
        client.getSegments(request, callback);
        return callback.getFuture().thenApply(segments -> {
            log.debug("Received the following data from the controller {}", segments);
            return segments.getSegmentsList()
                           .stream()
                           .collect(Collectors.toMap(location -> ModelHelper.encode(location.getSegmentId()),
                                                     location -> location.getOffset()));
        });
    }

    @Override
    public CompletableFuture<Map<Segment, List<Integer>>> getSuccessors(Segment segment) {
        long traceId = LoggerHelpers.traceEnter(log, "getSuccessors", segment);

        RPCAsyncCallback<SuccessorResponse> callback = new RPCAsyncCallback<>();
        client.getSegmentsImmediatlyFollowing(ModelHelper.decode(segment), callback);
        return callback.getFuture()
                       .thenApply(successors -> {
                           log.debug("Received the following data from the controller {}", successors);
                           Map<Segment, List<Integer>> result = new HashMap<>();
                           for (SuccessorResponse.SegmentEntry entry : successors.getSegmentsList()) {
                               result.put(ModelHelper.encode(entry.getSegmentId()), entry.getValueList());
                           }
                           return result;
                       })
                       .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "getSuccessors", traceId));
    }

    @Override
    public CompletableFuture<StreamSegments> getCurrentSegments(final String scope, final String stream) {
        long traceId = LoggerHelpers.traceEnter(log, "getCurrentSegments", scope, stream);
        Exceptions.checkNotNullOrEmpty(scope, "scope");
        Exceptions.checkNotNullOrEmpty(stream, "stream");

        RPCAsyncCallback<SegmentRanges> callback = new RPCAsyncCallback<>();
        client.getCurrentSegments(ModelHelper.createStreamInfo(scope, stream), callback);
        return callback.getFuture()
            .thenApply(ranges -> {
                NavigableMap<Double, Segment> rangeMap = new TreeMap<>();
                for (SegmentRange r : ranges.getSegmentRangesList()) {
                    rangeMap.put(r.getMaxKey(), ModelHelper.encode(r.getSegmentId()));
                }
                return rangeMap;
            })
            .thenApply(StreamSegments::new)
            .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "getCurrentSegments", traceId));
    }

    @Override
    public CompletableFuture<PravegaNodeUri> getEndpointForSegment(final String qualifiedSegmentName) {
        long traceId = LoggerHelpers.traceEnter(log, "getEndpointForSegment", qualifiedSegmentName);
        Exceptions.checkNotNullOrEmpty(qualifiedSegmentName, "qualifiedSegmentName");

        RPCAsyncCallback<NodeUri> callback = new RPCAsyncCallback<>();
        Segment segment = Segment.fromScopedName(qualifiedSegmentName);
        client.getURI(ModelHelper.createSegmentId(segment.getScope(),
                                                  segment.getStreamName(),
                                                  segment.getSegmentNumber()),
                      callback);
        return callback.getFuture().thenApply(ModelHelper::encode)
                .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "getEndpointForSegment", traceId));
    }

    @Override
    public CompletableFuture<Boolean> isSegmentOpen(final Segment segment) {
        long traceId = LoggerHelpers.traceEnter(log, "isSegmentOpen", segment);
        RPCAsyncCallback<SegmentValidityResponse> callback = new RPCAsyncCallback<>();
        client.isSegmentValid(ModelHelper.createSegmentId(segment.getScope(),
                                                          segment.getStreamName(),
                                                          segment.getSegmentNumber()),
                              callback);
        return callback.getFuture().thenApply(SegmentValidityResponse::getResponse)
                .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "isSegmentOpen", traceId));
    }

    @Override
    public CompletableFuture<UUID> createTransaction(final Stream stream, final long lease, final long maxExecutionTime,
                                                     final long scaleGracePeriod) {
        long traceId = LoggerHelpers.traceEnter(log, "createTransaction", stream, lease, maxExecutionTime, scaleGracePeriod);
        Preconditions.checkNotNull(stream, "stream");
        RPCAsyncCallback<TxnId> callback = new RPCAsyncCallback<>();
        client.createTransaction(CreateTxnRequest.newBuilder().setStreamInfo(
                ModelHelper.createStreamInfo(stream.getScope(), stream.getStreamName()))
                                         .setLease(lease)
                                         .setMaxExecutionTime(maxExecutionTime)
                                         .setScaleGracePeriod(scaleGracePeriod)
                                         .build(),
                                 callback);
        return callback.getFuture().thenApply(ModelHelper::encode)
                .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "createTransaction", traceId));
    }

    @Override
    public CompletableFuture<Void> pingTransaction(Stream stream, UUID txId, long lease) {
        long traceId = LoggerHelpers.traceEnter(log, "pingTransaction", stream, txId, lease);

        RPCAsyncCallback<PingTxnStatus> callback = new RPCAsyncCallback<>();
        client.pingTransaction(PingTxnRequest.newBuilder().setStreamInfo(
                ModelHelper.createStreamInfo(stream.getScope(), stream.getStreamName()))
                                       .setTxnId(ModelHelper.decode(txId))
                                       .setLease(lease).build(),
                               callback);
        return FutureHelpers.toVoidExpecting(callback.getFuture(),
                                             PingTxnStatus.newBuilder().setStatus(PingTxnStatus.Status.OK).build(),
                                             PingFailedException::new)
                .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "pingTransaction", traceId));
    }

    @Override
    public CompletableFuture<Void> commitTransaction(final Stream stream, final UUID txId) {
        long traceId = LoggerHelpers.traceEnter(log, "commitTransaction", stream, txId);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(txId, "txId");
        log.trace("Invoke AdminService.Client.commitTransaction() with stream: {}, txUd: {}", stream, txId);
        RPCAsyncCallback<TxnStatus> callback = new RPCAsyncCallback<>();
        client.commitTransaction(TxnRequest.newBuilder()
                                           .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(),
                                                                                       stream.getStreamName()))
                                           .setTxnId(ModelHelper.decode(txId))
                                           .build(),
                                 callback);

        return FutureHelpers.toVoidExpecting(callback.getFuture(),
                                             TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build(),
                                             TxnFailedException::new)
                            .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "commitTransaction", traceId));
    }

    @Override
    public CompletableFuture<Void> abortTransaction(final Stream stream, final UUID txId) {
        long traceId = LoggerHelpers.traceEnter(log, "abortTransaction", stream, txId);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(txId, "txId");
        log.trace("Invoke AdminService.Client.abortTransaction() with stream: {}, txUd: {}", stream, txId);
        RPCAsyncCallback<TxnStatus> callback = new RPCAsyncCallback<>();
        client.abortTransaction(TxnRequest.newBuilder()
                                          .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(),
                                                                                      stream.getStreamName()))
                                          .setTxnId(ModelHelper.decode(txId))
                                          .build(),
                                callback);
        return FutureHelpers.toVoidExpecting(callback.getFuture(),
                                             TxnStatus.newBuilder().setStatus(TxnStatus.Status.SUCCESS).build(),
                                             TxnFailedException::new)
                            .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "abortTransaction", traceId));
    }

    @Override
    public CompletableFuture<Transaction.Status> checkTransactionStatus(final Stream stream, final UUID txId) {
        long traceId = LoggerHelpers.traceEnter(log, "checkTransactionStatus", stream, txId);
        Preconditions.checkNotNull(stream, "stream");
        Preconditions.checkNotNull(txId, "txId");

        RPCAsyncCallback<TxnState> callback = new RPCAsyncCallback<>();
        client.checkTransactionState(TxnRequest.newBuilder()
                                               .setStreamInfo(ModelHelper.createStreamInfo(stream.getScope(),
                                                                                           stream.getStreamName()))
                                               .setTxnId(ModelHelper.decode(txId))
                                               .build(),
                                     callback);
        return callback.getFuture()
                       .thenApply(status -> ModelHelper.encode(status.getState(), stream + " " + txId))
                       .whenComplete((x, y) -> LoggerHelpers.traceLeave(log, "checkTransactionStatus", traceId));
    }

    // Local callback definition to wrap gRPC responses in CompletableFutures used by the rest of our code.
    private static final class RPCAsyncCallback<T> implements StreamObserver<T> {
        private T result = null;
        private final CompletableFuture<T> future = new CompletableFuture<>();

        @Override
        public void onNext(T value) {
            result = value;
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {

            // gRPC interface guarantees that onNext() with the success result would have been called before this.
            future.complete(result);
        }

        public CompletableFuture<T> getFuture() {
            return future;
        }
    }
}
