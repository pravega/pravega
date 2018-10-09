/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.rpc.grpc.v1;

import com.google.common.base.Throwables;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.pravega.auth.AuthHandler;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.common.Exceptions;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.rpc.auth.AuthHelper;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.DelegationToken;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.DeleteStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.GetSegmentsRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.NodeUri;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.PingTxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleStatusRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScaleStatusResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.ScopeInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentId;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentRanges;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentValidityResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime;
import io.pravega.controller.stream.api.grpc.v1.Controller.SegmentsAtTime.SegmentLocation;
import io.pravega.controller.stream.api.grpc.v1.Controller.ServerRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.ServerResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamConfig;
import io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo;
import io.pravega.controller.stream.api.grpc.v1.Controller.SuccessorResponse;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnRequest;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnState;
import io.pravega.controller.stream.api.grpc.v1.Controller.TxnStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.UpdateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.ControllerServiceGrpc;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * gRPC Service API implementation for the Controller.
 */
@Slf4j
@AllArgsConstructor
public class ControllerServiceImpl extends ControllerServiceGrpc.ControllerServiceImplBase {

    // The underlying Controller Service implementation to delegate all API calls to.
    private final ControllerService controllerService;
    private final AuthHelper authHelper;
    // Send to the client server traces on error message replies.
    private final boolean replyWithStackTraceOnError;

    @Override
    public void getControllerServerList(ServerRequest request, StreamObserver<ServerResponse> responseObserver) {
        log.info("getControllerServerList called.");
        authenticateExecuteAndProcessResults(() -> "",
                delegationToken -> controllerService.getControllerServerList()
                                     .thenApply(servers -> ServerResponse.newBuilder().addAllNodeURI(servers).build()),
                responseObserver);
    }

    @Override
    public void createStream(StreamConfig request, StreamObserver<CreateStreamStatus> responseObserver) {
        RequestTag requestTag = RequestTracker.initializeAndTrackRequestTag(System.nanoTime(), "createStream",
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());

        log.info("[requestId={}] createStream called for stream {}/{}.", requestTag.getRequestId(),
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorizationAndCreateToken(request.getStreamInfo().getScope()
                        + "/" + request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.createStream(ModelHelper.encode(request), requestTag.getRequestId()),
                responseObserver, requestTag);
    }

    @Override
    public void updateStream(StreamConfig request, StreamObserver<UpdateStreamStatus> responseObserver) {
        RequestTag requestTag = RequestTracker.initializeAndTrackRequestTag(System.nanoTime(), "updateStream",
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());

        log.info("[requestId={}] updateStream called for stream {}/{}.", requestTag.getRequestId(),
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.updateStream(ModelHelper.encode(request)), responseObserver, requestTag);
    }

    @Override
    public void truncateStream(Controller.StreamCut request, StreamObserver<UpdateStreamStatus> responseObserver) {
        RequestTag requestTag = RequestTracker.initializeAndTrackRequestTag(System.nanoTime(), "truncateStream",
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());

        log.info("[requestId={}] truncateStream called for stream {}/{}.", requestTag.getRequestId(),
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.truncateStream(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(), ModelHelper.encode(request)), responseObserver, requestTag);
    }

    @Override
    public void sealStream(StreamInfo request, StreamObserver<UpdateStreamStatus> responseObserver) {
        RequestTag requestTag = RequestTracker.initializeAndTrackRequestTag(System.nanoTime(), "sealStream",
                request.getScope(), request.getStream());

        log.info("[requestId={}] sealStream called for stream {}/{}.", requestTag.getRequestId(), request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(request.getScope() + "/" +
                        request.getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.sealStream(request.getScope(), request.getStream()), responseObserver, requestTag);
    }

    @Override
    public void deleteStream(StreamInfo request, StreamObserver<DeleteStreamStatus> responseObserver) {
        RequestTag requestTag = RequestTracker.initializeAndTrackRequestTag(System.nanoTime(), "deleteStream",
                request.getScope(), request.getStream());

        log.info("[requestId={}] deleteStream called for stream {}/{}.", requestTag.getRequestId(), request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(request.getScope() + "/" +
                        request.getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.deleteStream(request.getScope(), request.getStream()), responseObserver, requestTag);
    }

    @Override
    public void getCurrentSegments(StreamInfo request, StreamObserver<SegmentRanges> responseObserver) {
        log.info("getCurrentSegments called for stream {}/{}.", request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorizationAndCreateToken(request.getScope() + "/" +
                        request.getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.getCurrentSegments(request.getScope(), request.getStream())
                                       .thenApply(segmentRanges -> SegmentRanges.newBuilder()
                                                                                .addAllSegmentRanges(segmentRanges)
                                                                                .setDelegationToken(delegationToken)
                                                                                .build()),
                responseObserver);
    }

    @Override
    public void getSegments(GetSegmentsRequest request, StreamObserver<SegmentsAtTime> responseObserver) {
        log.debug("getSegments called for stream " + request.getStreamInfo().getScope() + "/" +
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorizationAndCreateToken(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.getSegmentsAtTime(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTimestamp())
                                       .thenApply(segments -> {
                                           SegmentsAtTime.Builder builder = SegmentsAtTime.newBuilder()
                                                                                          .setDelegationToken(delegationToken);
                                           for (Entry<SegmentId, Long> entry : segments.entrySet()) {
                                               builder.addSegments(SegmentLocation.newBuilder()
                                                                                  .setSegmentId(entry.getKey())
                                                                                  .setOffset(entry.getValue())
                                                                                  .build());
                                           }
                                           return builder.build();
                                       }),
                responseObserver);
    }

    @Override
    public void getSegmentsImmediatlyFollowing(SegmentId segmentId, StreamObserver<SuccessorResponse> responseObserver) {
        log.info("getSegmentsImmediatelyFollowing called for segment {} ", segmentId);
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(segmentId.getStreamInfo().getScope() + "/" +
                        segmentId.getStreamInfo().getStream(), AuthHandler.Permissions.READ),
                delegationToken -> controllerService.getSegmentsImmediatelyFollowing(segmentId)
                                       .thenApply(ModelHelper::createSuccessorResponse)
                                       .thenApply(response -> {
                                           response.setDelegationToken(delegationToken);
                                           return response.build();
                                       }),
                responseObserver);
    }

    @Override
    public void getSegmentsBetween(Controller.StreamCutRange request, StreamObserver<Controller.StreamCutRangeResponse> responseObserver) {
        log.info("getSegmentsBetweenStreamCuts called for stream {} for cuts from {} to {}", request.getStreamInfo(), request.getFromMap(), request.getToMap());
        String scope = request.getStreamInfo().getScope();
        String stream = request.getStreamInfo().getStream();
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(scope + "/" + stream, AuthHandler.Permissions.READ),
                delegationToken -> controllerService.getSegmentsBetweenStreamCuts(request)
                        .thenApply(segments -> ModelHelper.createStreamCutRangeResponse(scope, stream,
                                segments.stream().map(x -> ModelHelper.createSegmentId(scope, stream, x.segmentId()))
                                        .collect(Collectors.toList()), delegationToken)),
                responseObserver);
    }

    @Override
    public void scale(ScaleRequest request, StreamObserver<ScaleResponse> responseObserver) {
        log.info("[requestId={}] scale called for stream {}/{}.", request.getScaleTimestamp(), request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.scale(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getSealedSegmentsList(),
                        request.getNewKeyRangesList().stream().collect(Collectors.toMap(
                                entry -> entry.getStart(), entry -> entry.getEnd())),
                        request.getScaleTimestamp()),
                responseObserver);
    }

    @Override
    public void checkScale(ScaleStatusRequest request, StreamObserver<ScaleStatusResponse> responseObserver) {
        log.debug("check scale status called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.checkScale(request.getStreamInfo().getScope(), request.getStreamInfo().getStream(),
                        request.getEpoch()), responseObserver);
    }

    @Override
    public void getURI(SegmentId request, StreamObserver<NodeUri> responseObserver) {
        log.info("getURI called for segment {}/{}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getSegmentId());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.getURI(request),
                responseObserver);
    }

    @Override
    public void isSegmentValid(SegmentId request,
                               StreamObserver<SegmentValidityResponse> responseObserver) {
        log.info("isSegmentValid called for segment {}/{}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getSegmentId());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.isSegmentValid(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getSegmentId())
                                       .thenApply(bRes -> SegmentValidityResponse.newBuilder().setResponse(bRes).build()),
                responseObserver);
    }

    @Override
    public void isStreamCutValid(Controller.StreamCut request, StreamObserver<Controller.StreamCutValidityResponse> responseObserver) {
        log.info("isStreamCutValid called for stream {}/{} streamcut {}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getCutMap());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorizationAndCreateToken(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.isStreamCutValid(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getCutMap())
                        .thenApply(bRes -> Controller.StreamCutValidityResponse.newBuilder().setResponse(bRes).build()),
                responseObserver);
    }

    @Override
    public void createTransaction(CreateTxnRequest request, StreamObserver<Controller.CreateTxnResponse> responseObserver) {
        RequestTag requestTag = RequestTracker.initializeAndTrackRequestTag(System.nanoTime(), "createTransaction",
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());

        log.info("[requestId={}] createTransaction called for stream {}/{}.", requestTag.getRequestId(), request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorizationAndCreateToken(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.createTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getLease())
                                       .thenApply(pair -> Controller.CreateTxnResponse.newBuilder()
                                                                                      .setDelegationToken(delegationToken)
                                                                                      .setTxnId(ModelHelper.decode(pair.getKey()))
                                                                                      .addAllActiveSegments(pair.getValue())
                                                                                      .build()),
                responseObserver, requestTag);
    }

    @Override
    public void commitTransaction(TxnRequest request, StreamObserver<TxnStatus> responseObserver) {
        RequestTag requestTag = RequestTracker.initializeAndTrackRequestTag(System.nanoTime(), "commitTransaction",
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream(), request.getTxnId().toString());

        log.info("[requestId={}] commitTransaction called for stream {}/{}, txnId={}.", requestTag.getRequestId(),
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream(), request.getTxnId());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.commitTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId()),
                responseObserver, requestTag);
    }

    @Override
    public void abortTransaction(TxnRequest request, StreamObserver<TxnStatus> responseObserver) {
        RequestTag requestTag = RequestTracker.initializeAndTrackRequestTag(System.nanoTime(), "abortTransaction",
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());

        log.info("[requestId={}] abortTransaction called for stream {}/{}, txnId={}.", requestTag.getRequestId(),
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream(), request.getTxnId());
        authenticateExecuteAndProcessResults( () -> this.authHelper.checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.abortTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId()),
                responseObserver, requestTag);
    }

    @Override
    public void pingTransaction(PingTxnRequest request, StreamObserver<PingTxnStatus> responseObserver) {
        log.info("pingTransaction called for stream {}/{}, txnId={}", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ),
               delegationToken  -> controllerService.pingTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId(),
                        request.getLease()),
                responseObserver);
    }

    @Override
    public void checkTransactionState(TxnRequest request, StreamObserver<TxnState> responseObserver) {
        log.info("checkTransactionState called for stream {}/{}, txnId={}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.checkTransactionStatus(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId()),
                responseObserver);
    }

    @Override
    public void createScope(ScopeInfo request, StreamObserver<CreateScopeStatus> responseObserver) {
        RequestTag requestTag = RequestTracker.initializeAndTrackRequestTag(System.nanoTime(), "createScope", request.getScope());
        log.info("[requestId={}] createScope called for scope {}.", requestTag.getRequestId(), request.getScope());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(request.getScope(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.createScope(request.getScope()),
                responseObserver, requestTag);
    }

    @Override
    public void deleteScope(ScopeInfo request, StreamObserver<DeleteScopeStatus> responseObserver) {
        RequestTag requestTag = RequestTracker.initializeAndTrackRequestTag(System.nanoTime(), "deleteScope", request.getScope());
        log.info("[requestId={}] deleteScope called for scope {}.", requestTag.getRequestId(), request.getScope());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(request.getScope(), AuthHandler.Permissions.READ_UPDATE),
               delegationToken -> controllerService.deleteScope(request.getScope()),
                responseObserver, requestTag);
    }

    @Override
    public void getDelegationToken(StreamInfo request, StreamObserver<DelegationToken> responseObserver)  {
        log.info("getDelegationToken called for stream {}/{}.", request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorizationAndCreateToken(request.getScope() + "/" +
                        request.getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> CompletableFuture.completedFuture(Controller.DelegationToken
                        .newBuilder()
                        .setDelegationToken(delegationToken)
                        .build()),
                responseObserver);
    }

    // Convert responses from CompletableFuture to gRPC's Observer pattern.
    private <T> void authenticateExecuteAndProcessResults(Supplier<String> authenticator, Function<String, CompletableFuture<T>> call,
                                                          final StreamObserver<T> streamObserver, RequestTag requestTag) {
        try {
            String delegationToken;
            delegationToken = authenticator.get();
            CompletableFuture<T> result = call.apply(delegationToken);
            result.whenComplete(
                    (value, ex) -> {
                        log.debug("result =  {}", value);
                        logAndUntrackRequestTag(requestTag);
                        if (ex != null) {
                            Throwable cause = Exceptions.unwrap(ex);
                            log.error("Controller api failed with error: ", ex);
                            String errorDescription = replyWithStackTraceOnError ? "controllerStackTrace=" + Throwables.getStackTraceAsString(ex) : cause.getMessage();
                            streamObserver.onError(Status.INTERNAL
                                    .withCause(cause)
                                    .withDescription(errorDescription)
                                    .asRuntimeException());
                        } else if (value != null) {
                            streamObserver.onNext(value);
                            streamObserver.onCompleted();
                        }
                    });
        } catch (Exception e) {
            log.error("Controller api failed with authenticator error");
            logAndUntrackRequestTag(requestTag);
            streamObserver.onError(Status.UNAUTHENTICATED
                    .withDescription("Authentication failed")
                    .asRuntimeException());
        }
    }

    private <T> void authenticateExecuteAndProcessResults(Supplier<String> authenticator, Function<String, CompletableFuture<T>> call,
                                                          final StreamObserver<T> streamObserver) {
        authenticateExecuteAndProcessResults(authenticator, call, streamObserver, null);
    }

    private void logAndUntrackRequestTag(RequestTag requestTag) {
        if (requestTag != null) {
            log.info("[requestId={}] Untracking request: {}.", RequestTracker.getInstance().untrackRequest(requestTag.getRequestDescriptor()),
                    requestTag.getRequestDescriptor());
        }
    }
}
