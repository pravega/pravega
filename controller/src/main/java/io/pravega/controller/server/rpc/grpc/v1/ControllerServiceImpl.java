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

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.pravega.auth.AuthHandler;
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
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;
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
        authenticateExecuteAndProcessResults(aVoid -> true,
                (context) -> controllerService.getControllerServerList()
                                     .thenApply(servers -> ServerResponse.newBuilder().addAllNodeURI(servers).build()),
                responseObserver);
    }

    @Override
    public void createStream(StreamConfig request, StreamObserver<CreateStreamStatus> responseObserver) {
        log.info("createStream called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(v -> checkAuthorizationWithToken(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.createStream(ModelHelper.encode(request), System.currentTimeMillis()),
                responseObserver);
    }

    @Override
    public void updateStream(StreamConfig request, StreamObserver<UpdateStreamStatus> responseObserver) {
        log.info("updateStream called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.updateStream(ModelHelper.encode(request)), responseObserver);
    }

    @Override
    public void truncateStream(Controller.StreamCut request, StreamObserver<UpdateStreamStatus> responseObserver) {
        log.info("truncateStream called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.truncateStream(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(), ModelHelper.encode(request)), responseObserver);
    }

    @Override
    public void sealStream(StreamInfo request, StreamObserver<UpdateStreamStatus> responseObserver) {
        log.info("sealStream called for stream {}/{}.", request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getScope() + "/" +
                        request.getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.sealStream(request.getScope(), request.getStream()), responseObserver);
    }

    @Override
    public void deleteStream(StreamInfo request, StreamObserver<DeleteStreamStatus> responseObserver) {
        log.info("deleteStream called for stream {}/{}.", request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getScope() + "/" +
                        request.getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.deleteStream(request.getScope(), request.getStream()), responseObserver);
    }

    @Override
    public void getCurrentSegments(StreamInfo request, StreamObserver<SegmentRanges> responseObserver) {
        log.info("getCurrentSegments called for stream {}/{}.", request.getScope(), request.getStream());
        authenticateExecuteAndProcessResults(v -> checkAuthorizationWithToken(request.getScope() + "/" +
                        request.getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.getCurrentSegments(request.getScope(), request.getStream())
                                       .thenApply(segmentRanges -> SegmentRanges.newBuilder()
                                                                                .addAllSegmentRanges(segmentRanges)
                                                                                .setDelegationToken(getCurrentDelegationToken(context))
                                                                                .build()),
                responseObserver);
    }

    @Override
    public void getSegments(GetSegmentsRequest request, StreamObserver<SegmentsAtTime> responseObserver) {
        log.debug("getSegments called for stream " + request.getStreamInfo().getScope() + "/" +
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(v -> checkAuthorizationWithToken(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.getSegmentsAtTime(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTimestamp())
                                       .thenApply(segments -> {
                                           SegmentsAtTime.Builder builder = SegmentsAtTime.newBuilder()
                                                                                          .setDelegationToken(getCurrentDelegationToken(context));
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
        authenticateExecuteAndProcessResults(v -> checkAuthorization(segmentId.getStreamInfo().getScope() + "/" +
                        segmentId.getStreamInfo().getStream(), AuthHandler.Permissions.READ),
                (context) -> controllerService.getSegmentsImmediatelyFollowing(segmentId)
                                       .thenApply(ModelHelper::createSuccessorResponse)
                                       .thenApply(response -> {
                                           response.setDelegationToken(getCurrentDelegationToken(context));
                                           return response.build();
                                       }),
                responseObserver);
    }

    @Override
    public void getSegmentsBetween(Controller.StreamCutRange request, StreamObserver<Controller.StreamCutRangeResponse> responseObserver) {
        log.info("getSegmentsBetweenStreamCuts called for stream {} for cuts from {} to {}", request.getStreamInfo(), request.getFromMap(), request.getToMap());
        String scope = request.getStreamInfo().getScope();
        String stream = request.getStreamInfo().getStream();
        authenticateExecuteAndProcessResults(v -> checkAuthorization(scope + "/" + stream, AuthHandler.Permissions.READ),
                (context) -> controllerService.getSegmentsBetweenStreamCuts(request)
                        .thenApply(segments -> ModelHelper.createStreamCutRangeResponse(scope, stream,
                                segments.stream().map(x -> ModelHelper.createSegmentId(scope, stream, x.segmentId()))
                                        .collect(Collectors.toList()), getCurrentDelegationToken(context))),
                responseObserver);
    }

    @Override
    public void scale(ScaleRequest request, StreamObserver<ScaleResponse> responseObserver) {
        log.info("scale called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.scale(request.getStreamInfo().getScope(),
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
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.checkScale(request.getStreamInfo().getScope(), request.getStreamInfo().getStream(),
                        request.getEpoch()), responseObserver);
    }

    @Override
    public void getURI(SegmentId request, StreamObserver<NodeUri> responseObserver) {
        log.info("getURI called for segment {}/{}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getSegmentId());
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.getURI(request),
                responseObserver);
    }

    @Override
    public void isSegmentValid(SegmentId request,
                               StreamObserver<SegmentValidityResponse> responseObserver) {
        log.info("isSegmentValid called for segment {}/{}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getSegmentId());
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.isSegmentValid(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getSegmentId())
                                       .thenApply(bRes -> SegmentValidityResponse.newBuilder().setResponse(bRes).build()),
                responseObserver);
    }

    @Override
    public void createTransaction(CreateTxnRequest request, StreamObserver<Controller.CreateTxnResponse> responseObserver) {
        log.info("createTransaction called for stream {}/{}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream());
        authenticateExecuteAndProcessResults(v -> checkAuthorizationWithToken(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.createTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getLease())
                                       .thenApply(pair -> Controller.CreateTxnResponse.newBuilder()
                                                                                      .setDelegationToken(getCurrentDelegationToken(context))
                                                                                      .setTxnId(ModelHelper.decode(pair.getKey()))
                                                                                      .addAllActiveSegments(pair.getValue())
                                                                                      .build()),
                responseObserver);
    }

    @Override
    public void commitTransaction(TxnRequest request, StreamObserver<TxnStatus> responseObserver) {
        log.info("commitTransaction called for stream {}/{}, txnId={}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.commitTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId()),
                responseObserver);
    }

    @Override
    public void abortTransaction(TxnRequest request, StreamObserver<TxnStatus> responseObserver) {
        log.info("abortTransaction called for stream {}/{}, txnId={}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.abortTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId()),
                responseObserver);
    }

    @Override
    public void pingTransaction(PingTxnRequest request, StreamObserver<PingTxnStatus> responseObserver) {
        log.info("pingTransaction called for stream {}/{}, txnId={}", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ),
                (context) -> controllerService.pingTransaction(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId(),
                        request.getLease()),
                responseObserver);
    }

    @Override
    public void checkTransactionState(TxnRequest request, StreamObserver<TxnState> responseObserver) {
        log.info("checkTransactionState called for stream {}/{}, txnId={}.", request.getStreamInfo().getScope(),
                request.getStreamInfo().getStream(), request.getTxnId());
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getStreamInfo().getScope() + "/" +
                        request.getStreamInfo().getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.checkTransactionStatus(request.getStreamInfo().getScope(),
                        request.getStreamInfo().getStream(),
                        request.getTxnId()),
                responseObserver);
    }

    @Override
    public void createScope(ScopeInfo request,
                            StreamObserver<CreateScopeStatus> responseObserver) {
        log.info("createScope called for scope {}.", request.getScope());
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getScope(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.createScope(request.getScope()),
                responseObserver);
    }

    @Override
    public void deleteScope(ScopeInfo request,
                            StreamObserver<DeleteScopeStatus> responseObserver) {
        log.info("deleteScope called for scope {}.", request.getScope());
        authenticateExecuteAndProcessResults(v -> checkAuthorization(request.getScope(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> controllerService.deleteScope(request.getScope()),
                responseObserver);
    }

    @Override
    public void getDelegationToken(io.pravega.controller.stream.api.grpc.v1.Controller.StreamInfo request,
                                   io.grpc.stub.StreamObserver<io.pravega.controller.stream.api.grpc.v1.Controller.DelegationToken> responseObserver)  {
        log.info("createStream called for stream {}/{}.", request.getScope(),
                request.getStream());
        authenticateExecuteAndProcessResults(v -> checkAuthorizationWithToken(request.getScope() + "/" +
                        request.getStream(), AuthHandler.Permissions.READ_UPDATE),
                (context) -> CompletableFuture.completedFuture(Controller.DelegationToken
                        .newBuilder()
                        .setDelegationToken(getCurrentDelegationToken(context))
                        .build()),
                responseObserver);
    }

    // Convert responses from CompletableFuture to gRPC's Observer pattern.
    private static <T> void authenticateExecuteAndProcessResults(Predicate<Void> authenticator,
                                                                 Function<Context, CompletableFuture<T>> call, final StreamObserver<T> streamObserver) {
        if (authenticator.test(null)) {
            CompletableFuture<T> result = call.apply(Context.current());
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
        } else {
            log.error("Controller api failed with authenticator error");
            streamObserver.onError(Status.UNAUTHENTICATED
                    .withDescription("Authentication failed")
                    .asRuntimeException());
        }
    }

    public boolean checkAuthorization(String resource, AuthHandler.Permissions expectedLevel) {
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
                return false;
            }
            return true;
        } else {
            return true;
        }
    }

    public boolean checkAuthorizationWithToken(String resource, AuthHandler.Permissions expectedLevel) {
        if (isAuthEnabled) {
            boolean retVal = checkAuthorization(resource, expectedLevel);
            if (retVal) {
                PravegaInterceptor interceptor = PravegaInterceptor.INTERCEPTOR_OBJECT.get();
                interceptor.setDelegationToken(resource, expectedLevel, tokenSigningKey);
            }
            return retVal;
        } else {
            return true;
        }
    }

    private String getCurrentDelegationToken(Context context) {
        if (isAuthEnabled) {
            return PravegaInterceptor.retrieveDelegationToken(context);
        } else {
            return "";
        }
    }
}
