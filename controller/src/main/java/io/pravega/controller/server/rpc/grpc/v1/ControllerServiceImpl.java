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
import io.pravega.common.hash.RandomFactory;
import io.pravega.common.tracing.RequestTag;
import io.pravega.common.tracing.RequestTracker;
import io.pravega.common.tracing.TagLogger;
import io.pravega.controller.server.AuthResourceRepresentation;
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

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.slf4j.LoggerFactory;

/**
 * gRPC Service API implementation for the Controller.
 */
@AllArgsConstructor
public class ControllerServiceImpl extends ControllerServiceGrpc.ControllerServiceImplBase {

    private static final TagLogger log = new TagLogger(LoggerFactory.getLogger(ControllerServiceImpl.class));
    private static final int LIST_STREAMS_IN_SCOPE_LIMIT = 1000;

    // The underlying Controller Service implementation to delegate all API calls to.
    private final ControllerService controllerService;
    private final AuthHelper authHelper;
    private final RequestTracker requestTracker;
    // Send to the client server traces on error message replies.
    private final boolean replyWithStackTraceOnError;
    private final Supplier<Long> requestIdGenerator = RandomFactory.create()::nextLong;
    private final int listStreamsInScopeLimit;

    public ControllerServiceImpl(ControllerService controllerService, AuthHelper authHelper, RequestTracker requestTracker, boolean replyWithStackTraceOnError) {
        this(controllerService, authHelper, requestTracker, replyWithStackTraceOnError, LIST_STREAMS_IN_SCOPE_LIMIT);
    }

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
        String scope = request.getStreamInfo().getScope();
        String stream = request.getStreamInfo().getStream();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(requestIdGenerator.get(), "createStream",
                                                                            scope, stream);

        log.info(requestTag.getRequestId(), "createStream called for stream {}/{}.", scope, stream);
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorizationAndCreateToken(
                AuthResourceRepresentation.ofStreamsInScope(scope), AuthHandler.Permissions.READ_UPDATE),
                                             delegationToken -> controllerService.createStream(scope, stream,
                                                                                               ModelHelper.encode(request),
                                                                                               System.currentTimeMillis()),
                                             responseObserver, requestTag);
    }

    @Override
    public void updateStream(StreamConfig request, StreamObserver<UpdateStreamStatus> responseObserver) {
        String scope = request.getStreamInfo().getScope();
        String stream = request.getStreamInfo().getStream();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(requestIdGenerator.get(), "updateStream",
                scope, stream);

        log.info(requestTag.getRequestId(), "updateStream called for stream {}/{}.", scope, stream);
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofStreamInScope(scope, stream), AuthHandler.Permissions.READ_UPDATE),
                                             delegationToken -> controllerService.updateStream(scope, stream,
                                                                                               ModelHelper.encode(request)),
                                             responseObserver, requestTag);
    }

    @Override
    public void truncateStream(Controller.StreamCut request, StreamObserver<UpdateStreamStatus> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(requestIdGenerator.get(), "truncateStream",
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());

        log.info(requestTag.getRequestId(), "truncateStream called for stream {}/{}.",
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.truncateStream(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(), ModelHelper.encode(request)), responseObserver, requestTag);
    }

    @Override
    public void sealStream(StreamInfo request, StreamObserver<UpdateStreamStatus> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(requestIdGenerator.get(), "sealStream",
                request.getScope(), request.getStream());

        log.info(requestTag.getRequestId(), "sealStream called for stream {}/{}.",
                request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofStreamInScope(request.getScope(), request.getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.sealStream(request.getScope(), request.getStream()), responseObserver, requestTag);
    }

    @Override
    public void deleteStream(StreamInfo request, StreamObserver<DeleteStreamStatus> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(requestIdGenerator.get(), "deleteStream",
                request.getScope(), request.getStream());

        log.info(requestTag.getRequestId(), "deleteStream called for stream {}/{}.",
                request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofStreamInScope(request.getScope(), request.getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.deleteStream(request.getScope(), request.getStream()), responseObserver, requestTag);
    }

    @Override
    public void getCurrentSegments(StreamInfo request, StreamObserver<SegmentRanges> responseObserver) {
        log.info("getCurrentSegments called for stream {}/{}.", request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorizationAndCreateToken(
                AuthResourceRepresentation.ofStreamInScope(request.getScope(), request.getStream()),
                AuthHandler.Permissions.READ_UPDATE),
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
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorizationAndCreateToken(
                AuthResourceRepresentation.ofStreamInScope(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.getSegmentsAtHead(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream())
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
    public void getSegmentsImmediatelyFollowing(SegmentId segmentId, StreamObserver<SuccessorResponse> responseObserver) {
        log.info("getSegmentsImmediatelyFollowing called for segment {} ", segmentId);
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofStreamInScope(segmentId.getStreamInfo().getScope(),
                        segmentId.getStreamInfo().getStream()), AuthHandler.Permissions.READ),
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
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorizationAndCreateToken(
                AuthResourceRepresentation.ofStreamInScope(scope, stream), AuthHandler.Permissions.READ),
                delegationToken -> controllerService.getSegmentsBetweenStreamCuts(request)
                        .thenApply(segments -> ModelHelper.createStreamCutRangeResponse(scope, stream,
                                segments.stream().map(x -> ModelHelper.createSegmentId(scope, stream, x.segmentId()))
                                        .collect(Collectors.toList()), delegationToken)),
                responseObserver);
    }

    @Override
    public void scale(ScaleRequest request, StreamObserver<ScaleResponse> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(requestIdGenerator.get(), "scaleStream",
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream(), String.valueOf(request.getScaleTimestamp()));

        log.info(requestTag.getRequestId(), "scale called for stream {}/{}.",
                request.getStreamInfo().getScope(), request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ_UPDATE),
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
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ),
                delegationToken -> controllerService.checkScale(request.getStreamInfo().getScope(), request.getStreamInfo().getStream(),
                        request.getEpoch()), responseObserver);
    }

    @Override
    public void getURI(SegmentId request, StreamObserver<NodeUri> responseObserver) {
        log.info("getURI called for segment {}/{}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getSegmentId());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ),
                delegationToken -> controllerService.getURI(request),
                responseObserver);
    }

    @Override
    public void isSegmentValid(SegmentId request,
                               StreamObserver<SegmentValidityResponse> responseObserver) {
        log.info("isSegmentValid called for segment {}/{}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getSegmentId());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofStreamInScope(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ),
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
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorizationAndCreateToken(
                AuthResourceRepresentation.ofStreamInScope(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ),
                delegationToken -> controllerService.isStreamCutValid(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getCutMap())
                        .thenApply(bRes -> Controller.StreamCutValidityResponse.newBuilder().setResponse(bRes).build()),
                responseObserver);
    }

    @Override
    public void createTransaction(CreateTxnRequest request, StreamObserver<Controller.CreateTxnResponse> responseObserver) {
        log.info("createTransaction called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorizationAndCreateToken(
                AuthResourceRepresentation.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.createTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getLease())
                                       .thenApply(pair -> Controller.CreateTxnResponse.newBuilder()
                                                                                      .setDelegationToken(delegationToken)
                                                                                      .setTxnId(ModelHelper.decode(pair.getKey()))
                                                                                      .addAllActiveSegments(pair.getValue())
                                                                                      .build()),
                responseObserver);
    }

    @Override
    public void commitTransaction(TxnRequest request, StreamObserver<TxnStatus> responseObserver) {
        log.info("commitTransaction called for stream {}/{}, txnId={}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofStreamInScope(
                        request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.commitTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId()),
                responseObserver);
    }

    @Override
    public void abortTransaction(TxnRequest request, StreamObserver<TxnStatus> responseObserver) {
        log.info("abortTransaction called for stream {}/{}, txnId={}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        authenticateExecuteAndProcessResults( () -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.abortTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId()),
                responseObserver);
    }

    @Override
    public void pingTransaction(PingTxnRequest request, StreamObserver<PingTxnStatus> responseObserver) {
        log.info("pingTransaction called for stream {}/{}, txnId={}", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ_UPDATE),
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
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofStreamInScope(request.getStreamInfo().getScope(), request.getStreamInfo().getStream()),
                AuthHandler.Permissions.READ),
                delegationToken -> controllerService.checkTransactionStatus(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId()),
                responseObserver);
    }

    @Override
    public void createScope(ScopeInfo request, StreamObserver<CreateScopeStatus> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(requestIdGenerator.get(), "createScope", request.getScope());
        log.info(requestTag.getRequestId(), "createScope called for scope {}.", request.getScope());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofScopes(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.createScope(request.getScope()),
                responseObserver, requestTag);
    }

    @Override
    public void listStreamsInScope(Controller.StreamsInScopeRequest request, StreamObserver<Controller.StreamsInScopeResponse> responseObserver) {
        String scope = request.getScope().getScope();
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(requestIdGenerator.get(), "listStream", scope);
        log.info(requestTag.getRequestId(), "listStream called for scope {}.", scope);
        authenticateExecuteAndProcessResults(
                () -> this.authHelper.checkAuthorization(AuthResourceRepresentation.ofScope(scope),
                        AuthHandler.Permissions.READ),
                delegationToken -> controllerService.listStreams(
                        scope, request.getContinuationToken().getToken(), listStreamsInScopeLimit)
                        .thenApply(response -> {
                             List<StreamInfo> streams = response.getKey().stream()
                                     .filter(m -> authHelper.isAuthorized(m, AuthHandler.Permissions.READ))
                                     .map(m -> StreamInfo.newBuilder().setScope(scope).setStream(m).build())
                                     .collect(Collectors.toList());
                             return Controller.StreamsInScopeResponse.newBuilder().addAllStreams(streams)
                                     .setContinuationToken(Controller.ContinuationToken.newBuilder().setToken(
                                             response.getValue()).build()).build();
                        }),
                        responseObserver, requestTag);
    }

    @Override
    public void deleteScope(ScopeInfo request, StreamObserver<DeleteScopeStatus> responseObserver) {
        RequestTag requestTag = requestTracker.initializeAndTrackRequestTag(requestIdGenerator.get(), "deleteScope", request.getScope());
        log.info(requestTag.getRequestId(), "deleteScope called for scope {}.", request.getScope());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorization(
                AuthResourceRepresentation.ofScopes(), AuthHandler.Permissions.READ_UPDATE),
               delegationToken -> controllerService.deleteScope(request.getScope()),
                responseObserver, requestTag);
    }

    @Override
    public void getDelegationToken(StreamInfo request, StreamObserver<DelegationToken> responseObserver)  {
        log.info("getDelegationToken called for stream {}/{}.", request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(() -> this.authHelper.checkAuthorizationAndCreateToken(
                AuthResourceRepresentation.ofStreamInScope(request.getScope(), request.getStream()),
                AuthHandler.Permissions.READ_UPDATE),
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
                            log.error("Controller api failed with error: {}", ex.getMessage(), ex);
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
            log.error(e.getMessage(), e);
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
            log.debug(requestTracker.untrackRequest(requestTag.getRequestDescriptor()),
                    "Untracking request: {}.", requestTag.getRequestDescriptor());
        }
    }
}
