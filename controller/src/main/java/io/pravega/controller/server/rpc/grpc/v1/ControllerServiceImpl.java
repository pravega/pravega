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

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.pravega.auth.AuthHandler;
import io.pravega.auth.AuthorizationException;
import io.pravega.client.stream.impl.ModelHelper;
import io.pravega.common.Exceptions;
import io.pravega.controller.server.ControllerService;
import io.pravega.controller.server.rpc.auth.PravegaInterceptor;
import io.pravega.controller.stream.api.grpc.v1.Controller;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateScopeStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateStreamStatus;
import io.pravega.controller.stream.api.grpc.v1.Controller.CreateTxnRequest;
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
import java.util.HashMap;
import java.util.Map;
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
    private final String tokenSigningKey;
    private final boolean isAuthEnabled;

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
        log.info("createStream called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> checkAuthorizationAndCreateToken(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.createStream(ModelHelper.encode(request), System.currentTimeMillis()),
                responseObserver);
    }

    @Override
    public void updateStream(StreamConfig request, StreamObserver<UpdateStreamStatus> responseObserver) {
        log.info("updateStream called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.updateStream(ModelHelper.encode(request)), responseObserver);
    }

    @Override
    public void truncateStream(Controller.StreamCut request, StreamObserver<UpdateStreamStatus> responseObserver) {
        log.info("truncateStream called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.truncateStream(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(), ModelHelper.encode(request)), responseObserver);
    }

    @Override
    public void sealStream(StreamInfo request, StreamObserver<UpdateStreamStatus> responseObserver) {
        log.info("sealStream called for stream {}/{}.", request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(() -> checkAuthorization(request.getScope() + "/" +
                        request.getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.sealStream(request.getScope(), request.getStream()), responseObserver);
    }

    @Override
    public void deleteStream(StreamInfo request, StreamObserver<DeleteStreamStatus> responseObserver) {
        log.info("deleteStream called for stream {}/{}.", request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(() -> checkAuthorization(request.getScope() + "/" +
                        request.getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.deleteStream(request.getScope(), request.getStream()), responseObserver);
    }

    @Override
    public void getCurrentSegments(StreamInfo request, StreamObserver<SegmentRanges> responseObserver) {
        log.info("getCurrentSegments called for stream {}/{}.", request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(() -> checkAuthorizationAndCreateToken(request.getScope() + "/" +
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
        authenticateExecuteAndProcessResults(() -> checkAuthorizationAndCreateToken(request.getStreamInfo().getScope() + "/" +
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
    public void getSegmentsImmediatlyFollowing(SegmentId segmentId,
                                               StreamObserver<SuccessorResponse> responseObserver) {
        log.info("getSegmentsImmediatelyFollowing called for segment {} ", segmentId);
        authenticateExecuteAndProcessResults(() -> checkAuthorization(segmentId.getStreamInfo().getScope() + "/" +
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
        authenticateExecuteAndProcessResults(() -> checkAuthorization(scope + "/" + stream, AuthHandler.Permissions.READ),
                delegationToken -> controllerService.getSegmentsBetweenStreamCuts(request)
                        .thenApply(segments -> ModelHelper.createStreamCutRangeResponse(scope, stream,
                                segments.stream().map(x -> ModelHelper.createSegmentId(scope, stream, x.segmentId()))
                                        .collect(Collectors.toList()), delegationToken)),
                responseObserver);
    }

    @Override
    public void scale(ScaleRequest request, StreamObserver<ScaleResponse> responseObserver) {
        log.info("scale called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
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
        authenticateExecuteAndProcessResults(() -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.checkScale(request.getStreamInfo().getScope(), request.getStreamInfo().getStream(),
                        request.getEpoch()), responseObserver);
    }

    @Override
    public void getURI(SegmentId request, StreamObserver<NodeUri> responseObserver) {
        log.info("getURI called for segment {}/{}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getSegmentId());
        authenticateExecuteAndProcessResults(() -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.getURI(request),
                responseObserver);
    }

    @Override
    public void isSegmentValid(SegmentId request,
                               StreamObserver<SegmentValidityResponse> responseObserver) {
        log.info("isSegmentValid called for segment {}/{}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getSegmentId());
        authenticateExecuteAndProcessResults(() -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.isSegmentValid(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getSegmentId())
                                       .thenApply(bRes -> SegmentValidityResponse.newBuilder().setResponse(bRes).build()),
                responseObserver);
    }

    @Override
    public void createTransaction(CreateTxnRequest request, StreamObserver<Controller.CreateTxnResponse> responseObserver) {
        log.info("createTransaction called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(() -> checkAuthorizationAndCreateToken(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
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
        authenticateExecuteAndProcessResults(() -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.commitTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId()),
                responseObserver);
    }

    @Override
    public void abortTransaction(TxnRequest request, StreamObserver<TxnStatus> responseObserver) {
        log.info("abortTransaction called for stream {}/{}, txnId={}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        authenticateExecuteAndProcessResults( () -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.abortTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId()),
                responseObserver);
    }

    @Override
    public void pingTransaction(PingTxnRequest request, StreamObserver<PingTxnStatus> responseObserver) {
        log.info("pingTransaction called for stream {}/{}, txnId={}", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        authenticateExecuteAndProcessResults(() -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
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
        authenticateExecuteAndProcessResults(() -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.checkTransactionStatus(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId()),
                responseObserver);
    }

    @Override
    public void createScope(ScopeInfo request,
                            StreamObserver<CreateScopeStatus> responseObserver) {
        log.info("createScope called for scope {}.", request.getScope());
        authenticateExecuteAndProcessResults(() -> checkAuthorization(request.getScope(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> controllerService.createScope(request.getScope()),
                responseObserver);
    }

    @Override
    public void deleteScope(ScopeInfo request,
                            StreamObserver<DeleteScopeStatus> responseObserver) {
        log.info("deleteScope called for scope {}.", request.getScope());
        authenticateExecuteAndProcessResults(() -> checkAuthorization(request.getScope(), AuthHandler.Permissions.READ_UPDATE),
               delegationToken -> controllerService.deleteScope(request.getScope()),
                responseObserver);
    }

    @Override
    public void getDelegationToken(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request,
                                   io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.DelegationToken> responseObserver)  {
        log.info("createStream called for stream {}/{}.", request.getScope(),
                request.getStream());
        authenticateExecuteAndProcessResults(() -> checkAuthorizationAndCreateToken(request.getScope() + "/" +
                        request.getStream(), AuthHandler.Permissions.READ_UPDATE),
                delegationToken -> CompletableFuture.completedFuture(Controller.DelegationToken
                        .newBuilder()
                        .setDelegationToken(delegationToken)
                        .build()),
                responseObserver);
    }

    // Convert responses from CompletableFuture to gRPC's Observer pattern.
    private static <T> void authenticateExecuteAndProcessResults(Supplier<String> authenticator,
                                                                 Function<String, CompletableFuture<T>> call, final StreamObserver<T> streamObserver) {
        try {
            String delegationToken;
            delegationToken = authenticator.get();
            CompletableFuture<T> result = call.apply(delegationToken);
            result.whenComplete(
                    (value, ex) -> {
                        log.debug("result =  {}", value);

                        if (ex != null) {
                            Throwable cause = Exceptions.unwrap(ex);
                            log.error("Controller api failed with error: ", ex);
                            streamObserver.onError(Status.INTERNAL
                                    .withCause(cause)
                                    .withDescription(cause.getMessage())
                                    .asRuntimeException());
                        } else if (value != null) {
                            streamObserver.onNext(value);
                            streamObserver.onCompleted();
                        }
                    });
        } catch (Exception e) {
            log.error("Controller api failed with authenticator error");
            streamObserver.onError(Status.UNAUTHENTICATED
                    .withDescription("Authentication failed")
                    .asRuntimeException());
        }
    }

    private String checkAuthorization(String resource, AuthHandler.Permissions expectedLevel) throws AuthorizationException {
        if (isAuthEnabled) {
            PravegaInterceptor currentInterceptor = PravegaInterceptor.INTERCEPTOR_OBJECT.get();

            AuthHandler.Permissions allowedLevel;
            if (currentInterceptor == null) {
                //No interceptor, and authorization is enabled. Means no access is granted.
                allowedLevel = AuthHandler.Permissions.NONE;
            } else {
                allowedLevel = currentInterceptor.authorize(resource);
            }
            if (allowedLevel.ordinal() < expectedLevel.ordinal()) {
                throw new AuthorizationException("Access not allowed");
            }
        }
        return "";
    }

    private String checkAuthorizationAndCreateToken(String resource, AuthHandler.Permissions expectedLevel) throws AuthorizationException {
        if (isAuthEnabled) {
            checkAuthorization(resource, expectedLevel);
            return createDelegationToken(resource, expectedLevel, tokenSigningKey);
        }
        return "";
    }

    private String createDelegationToken(String resource, AuthHandler.Permissions expectedLevel, String tokenSigningKey) {
        if (isAuthEnabled) {
            Map<String, Object> claims = new HashMap<>();

            claims.put(resource, String.valueOf(expectedLevel));

            return Jwts.builder()
                       .setSubject("segmentstoreresource")
                       .setAudience("segmentstore")
                       .setClaims(claims)
                       .signWith(SignatureAlgorithm.HS512, tokenSigningKey.getBytes())
                       .compact();
        }
        return "";
    }
}
